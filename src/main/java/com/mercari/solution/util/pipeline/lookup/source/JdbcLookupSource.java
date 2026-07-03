package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * JDBC-backed {@link LookupSource}: answers key-driven lookups against database
 * tables with a single parameterized query per batch — an OR of equality tuples
 * over the distinct key prefixes ({@code (k1=? AND k2=?) OR ...}), plus a global
 * range bound (a superset; the lookup-join operator filters exactly).
 *
 * <p>Table metadata (columns, primary key, unique indexes) is read once from
 * {@link DatabaseMetaData} — at pipeline construction time — and serialized with
 * the source, so workers do not repeat the derivation. Key candidates are the
 * primary key first, then unique secondary indexes. Identifiers are always
 * quoted with the database's quote string; configure table/column names in the
 * exact case the database stores them (e.g. uppercase for H2).
 */
public class JdbcLookupSource extends LookupSource {

    /** One exposed table: the SQL name and the physical (optionally schema-qualified) name. */
    private record TableConfig(String name, String table) implements Serializable {
    }

    /** Serializable per-table metadata derived from {@link DatabaseMetaData}. */
    private static class TableMeta implements Serializable {

        private final String physicalTable;
        private final Schema schema;
        private final List<LookupKey> keyCandidates;

        private TableMeta(String physicalTable, Schema schema, List<LookupKey> keyCandidates) {
            this.physicalTable = physicalTable;
            this.schema = schema;
            this.keyCandidates = keyCandidates;
        }

        private Schema.Type columnType(int ordinal) {
            return schema.getField(ordinal).getFieldType().getType();
        }

        private Schema.Type columnType(String name) {
            for (int i = 0; i < schema.countFields(); i++) {
                if (schema.getField(i).getName().equals(name)) {
                    return columnType(i);
                }
            }
            throw new IllegalStateException("unknown column: " + name);
        }

        private String columnName(int ordinal) {
            return schema.getField(ordinal).getName();
        }
    }

    private final String driver;
    private final String url;
    private final String user;
    private final String password;
    private final List<TableConfig> tables;

    // Derived once (at pipeline construction) and serialized to workers.
    private Map<String, TableMeta> meta;
    private String quote;

    private transient JdbcUtil.CloseableDataSource dataSource;

    private JdbcLookupSource(Builder builder) {
        super(builder.name);
        this.driver = Objects.requireNonNull(builder.driver, "jdbc driver must not be null");
        this.url = Objects.requireNonNull(builder.url, "jdbc url must not be null");
        this.user = builder.user;
        this.password = builder.password;
        this.tables = List.copyOf(builder.tables);
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("jdbc lookup source requires at least one table");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private String driver;
        private String url;
        private String user;
        private String password;
        private final List<TableConfig> tables = new ArrayList<>();

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withDriver(String driver) {
            this.driver = driver;
            return this;
        }

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withTable(String name) {
            return withTable(name, name);
        }

        /** Exposes physical table {@code table} (optionally {@code schema.table}) as {@code name}. */
        public Builder withTable(String name, String table) {
            this.tables.add(new TableConfig(name, table));
            return this;
        }

        public JdbcLookupSource build() {
            return new JdbcLookupSource(this);
        }
    }

    @Override
    protected void setupInternal() {
        if (dataSource == null) {
            dataSource = JdbcUtil.createDataSource(driver, url, user, password, true);
        }
        if (meta == null) {
            deriveMeta();
        }
    }

    @Override
    protected void closeInternal() {
        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (Exception e) {
                throw new IllegalStateException(
                        "failed to close jdbc lookup source: " + getName(), e);
            } finally {
                dataSource = null;
            }
        }
    }

    @Override
    public Map<String, Schema> tableSchemas() {
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        for (final TableConfig table : tables) {
            schemas.put(table.name(), requireMeta(table.name()).schema);
        }
        return schemas;
    }

    @Override
    public List<LookupKey> keyCandidates(String table) {
        final TableMeta tableMeta = meta == null ? null : meta.get(table);
        return tableMeta == null ? List.of() : tableMeta.keyCandidates;
    }

    @Override
    public boolean supportsKeyPrefixLookup() {
        // Prefix-only equality becomes an index-backed OR-of-tuples query.
        return true;
    }

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        final TableMeta tableMeta = requireMeta(table);
        final List<String> keyFields = keyColumns(tableMeta, indexName);
        final int prefixLen = batch.prefixLength();
        final List<List<Object>> prefixes = batch.distinctPrefixes();
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(tableMeta.schema.countFields());

        final String sql = buildSql(tableMeta, keyFields, prefixLen, prefixes.size(),
                batch.isRange(), outCols);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            int p = 1;
            if (prefixLen > 0) {
                for (final List<Object> prefix : prefixes) {
                    for (int i = 0; i < prefixLen; i++) {
                        bind(ps, p++, prefix.get(i), tableMeta.columnType(keyFields.get(i)));
                    }
                }
            }
            if (batch.isRange()) {
                final Schema.Type rangeType = tableMeta.columnType(keyFields.get(prefixLen));
                bind(ps, p++, batch.globalLower(), rangeType);
                bind(ps, p, batch.globalUpper(), rangeType);
            }
            final List<Object[]> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final Object[] row = new Object[outCols.length];
                    for (int i = 0; i < outCols.length; i++) {
                        row[i] = extract(rs, i + 1, tableMeta.columnType(outCols[i]));
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new IllegalStateException("JDBC lookup failed on table " + table, e);
        }
    }

    private String buildSql(TableMeta tableMeta, List<String> keyFields, int prefixLen,
            int prefixCount, boolean range, int[] outCols) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < outCols.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quote(tableMeta.columnName(outCols[i])));
        }
        sql.append(" FROM ").append(quotePhysical(tableMeta.physicalTable)).append(" WHERE ");

        boolean needAnd = false;
        if (prefixLen > 0) {
            sql.append('(');
            for (int t = 0; t < prefixCount; t++) {
                if (t > 0) {
                    sql.append(" OR ");
                }
                sql.append('(');
                for (int i = 0; i < prefixLen; i++) {
                    if (i > 0) {
                        sql.append(" AND ");
                    }
                    sql.append(quote(keyFields.get(i))).append(" = ?");
                }
                sql.append(')');
            }
            sql.append(')');
            needAnd = true;
        }
        if (range) {
            if (needAnd) {
                sql.append(" AND ");
            }
            final String rangeCol = quote(keyFields.get(prefixLen));
            sql.append(rangeCol).append(" >= ? AND ").append(rangeCol).append(" <= ?");
        }
        return sql.toString();
    }

    private void deriveMeta() {
        final Map<String, TableMeta> derived = new LinkedHashMap<>();
        try (Connection connection = dataSource.getConnection()) {
            final DatabaseMetaData databaseMeta = connection.getMetaData();
            final String quoteString = databaseMeta.getIdentifierQuoteString();
            this.quote = quoteString == null || quoteString.isBlank() ? "\"" : quoteString.trim();
            for (final TableConfig table : tables) {
                derived.put(table.name(), readTableMeta(databaseMeta, table));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "failed to read jdbc metadata for lookup source: " + getName(), e);
        }
        this.meta = derived;
    }

    private static TableMeta readTableMeta(DatabaseMetaData databaseMeta, TableConfig table)
            throws SQLException {
        final String physical = table.table();
        final int dot = physical.indexOf('.');
        final String schemaPattern = dot < 0 ? null : physical.substring(0, dot);
        final String tableName = dot < 0 ? physical : physical.substring(dot + 1);

        final Schema.Builder schemaBuilder = Schema.builder();
        int columnCount = 0;
        try (ResultSet rs = databaseMeta.getColumns(null, schemaPattern, tableName, null)) {
            while (rs.next()) {
                schemaBuilder.withField(rs.getString("COLUMN_NAME"),
                        toFieldType(rs.getInt("DATA_TYPE")));
                columnCount++;
            }
        }
        if (columnCount == 0) {
            throw new IllegalStateException("JDBC table not found for lookup: " + physical);
        }

        final List<String> primaryKey = primaryKey(databaseMeta, schemaPattern, tableName);
        final List<LookupKey> candidates = new ArrayList<>();
        if (!primaryKey.isEmpty()) {
            candidates.add(LookupKey.primaryKey(primaryKey));
        }
        for (final Map.Entry<String, List<String>> index
                : uniqueIndexes(databaseMeta, schemaPattern, tableName).entrySet()) {
            if (!index.getValue().equals(primaryKey)) { // skip the index backing the PK
                candidates.add(LookupKey.index(index.getKey(), index.getValue()));
            }
        }
        return new TableMeta(physical, schemaBuilder.build(), candidates);
    }

    private static List<String> primaryKey(DatabaseMetaData meta, String schema, String table)
            throws SQLException {
        // KEY_SEQ orders the key columns; collect then sort by it.
        final List<int[]> seqIndex = new ArrayList<>();
        final List<String> cols = new ArrayList<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            int i = 0;
            while (rs.next()) {
                cols.add(rs.getString("COLUMN_NAME"));
                seqIndex.add(new int[]{rs.getShort("KEY_SEQ"), i++});
            }
        }
        seqIndex.sort((a, b) -> Integer.compare(a[0], b[0]));
        final List<String> ordered = new ArrayList<>();
        for (final int[] pair : seqIndex) {
            ordered.add(cols.get(pair[1]));
        }
        return ordered;
    }

    /** Unique secondary indexes: index name → columns in ORDINAL_POSITION order. */
    private static Map<String, List<String>> uniqueIndexes(DatabaseMetaData meta, String schema,
            String table) throws SQLException {
        final Map<String, List<int[]>> orderByIndex = new LinkedHashMap<>();
        final Map<String, List<String>> colsByIndex = new LinkedHashMap<>();
        try (ResultSet rs = meta.getIndexInfo(null, schema, table, true, true)) {
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    continue;
                }
                final String indexName = rs.getString("INDEX_NAME");
                final String columnName = rs.getString("COLUMN_NAME");
                if (indexName == null || columnName == null) {
                    continue;
                }
                final List<String> cols =
                        colsByIndex.computeIfAbsent(indexName, k -> new ArrayList<>());
                orderByIndex.computeIfAbsent(indexName, k -> new ArrayList<>())
                        .add(new int[]{rs.getShort("ORDINAL_POSITION"), cols.size()});
                cols.add(columnName);
            }
        }
        final Map<String, List<String>> ordered = new LinkedHashMap<>();
        for (final Map.Entry<String, List<int[]>> e : orderByIndex.entrySet()) {
            final List<int[]> seq = new ArrayList<>(e.getValue());
            seq.sort((a, b) -> Integer.compare(a[0], b[0]));
            final List<String> cols = colsByIndex.get(e.getKey());
            final List<String> result = new ArrayList<>();
            for (final int[] pair : seq) {
                result.add(cols.get(pair[1]));
            }
            ordered.put(e.getKey(), result);
        }
        return ordered;
    }

    private static Schema.FieldType toFieldType(int jdbcType) {
        return switch (jdbcType) {
            case Types.BIT, Types.BOOLEAN -> Schema.FieldType.BOOLEAN;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Schema.FieldType.INT32;
            case Types.BIGINT -> Schema.FieldType.INT64;
            case Types.REAL, Types.FLOAT, Types.DOUBLE -> Schema.FieldType.FLOAT64;
            case Types.DECIMAL, Types.NUMERIC -> Schema.FieldType.DECIMAL;
            case Types.DATE -> Schema.FieldType.DATE;
            case Types.TIME -> Schema.FieldType.TIME;
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> Schema.FieldType.TIMESTAMP;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB ->
                    Schema.FieldType.BYTES;
            default -> Schema.FieldType.STRING;
        };
    }

    private static void bind(PreparedStatement ps, int idx, Object value, Schema.Type type)
            throws SQLException {
        if (value == null) {
            ps.setObject(idx, null);
            return;
        }
        switch (type) {
            case string, json -> ps.setString(idx, value.toString());
            case int8, int16, int32 -> ps.setInt(idx, ((Number) value).intValue());
            case int64 -> ps.setLong(idx, ((Number) value).longValue());
            case float32, float64 -> ps.setDouble(idx, ((Number) value).doubleValue());
            case decimal -> ps.setBigDecimal(idx, (BigDecimal) value);
            case bool -> ps.setBoolean(idx, (Boolean) value);
            case date -> ps.setDate(idx,
                    Date.valueOf(LocalDate.ofEpochDay(((Number) value).longValue())));
            case time -> ps.setTime(idx, Time.valueOf(
                    LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L)));
            case timestamp -> ps.setTimestamp(idx, new Timestamp(((Number) value).longValue()));
            case bytes -> ps.setBytes(idx, ((ByteString) value).getBytes());
            default -> ps.setObject(idx, value);
        }
    }

    /** Extracts a column as its Calcite-internal value. */
    private static Object extract(ResultSet rs, int col, Schema.Type type) throws SQLException {
        final Object value = switch (type) {
            case string, json -> rs.getString(col);
            case int8, int16, int32 -> rs.getInt(col);
            case int64 -> rs.getLong(col);
            case float32, float64 -> rs.getDouble(col);
            case decimal -> rs.getBigDecimal(col);
            case bool -> rs.getBoolean(col);
            case date -> {
                final Date d = rs.getDate(col);
                yield d == null ? null : (int) d.toLocalDate().toEpochDay();
            }
            case time -> {
                final Time t = rs.getTime(col);
                yield t == null ? null : (int) (t.toLocalTime().toNanoOfDay() / 1_000_000L);
            }
            case timestamp -> {
                final Timestamp ts = rs.getTimestamp(col);
                yield ts == null ? null : ts.getTime();
            }
            case bytes -> {
                final byte[] bytes = rs.getBytes(col);
                yield bytes == null ? null : new ByteString(bytes);
            }
            default -> rs.getObject(col);
        };
        return rs.wasNull() ? null : value;
    }

    private static List<String> keyColumns(TableMeta tableMeta, String indexName) {
        for (final LookupKey candidate : tableMeta.keyCandidates) {
            if (Objects.equals(candidate.indexName(), indexName)) {
                return candidate.columns();
            }
        }
        throw new IllegalStateException("Unknown key for lookup: " + indexName);
    }

    private TableMeta requireMeta(String table) {
        if (meta == null) {
            throw new IllegalStateException(
                    "jdbc lookup source '" + getName() + "' is not set up");
        }
        final TableMeta tableMeta = meta.get(table);
        if (tableMeta == null) {
            throw new IllegalStateException("unknown lookup table: " + getName() + "." + table);
        }
        return tableMeta;
    }

    private String quote(String identifier) {
        return quote + identifier + quote;
    }

    private String quotePhysical(String physical) {
        final int dot = physical.indexOf('.');
        return dot < 0 ? quote(physical)
                : quote(physical.substring(0, dot)) + "." + quote(physical.substring(dot + 1));
    }
}
