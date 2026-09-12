package com.mercari.solution.util.domain.db;

import com.google.common.net.InetAddresses;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.cloud.SecretProviders;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.AvroToJsonConverter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.options.PipelineOptions;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utility for transferring data with PostgreSQL (or compatible) databases
 * using {@code COPY ... WITH (FORMAT BINARY)} via the JDBC driver's CopyManager API.
 */
public class PostgresUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresUtil.class);

    public static final String DRIVER = "org.postgresql.Driver";

    private static final byte[] COPY_BINARY_SIGNATURE = {
            'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0
    };

    // 2000-01-01T00:00:00Z (PostgreSQL epoch) relative to unix epoch
    private static final long POSTGRES_EPOCH_MICROS = 946684800000000L;
    private static final int POSTGRES_EPOCH_DAYS = 10957;
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    // address family codes in the inet/cidr binary format (utils/inet.h)
    private static final int PGSQL_AF_INET = 2;
    private static final int PGSQL_AF_INET6 = 3;

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
        TIMETZ,
        TIMESTAMP,
        TIMESTAMPTZ,
        UUID,
        JSON,
        JSONB,
        XML,
        INET,
        CIDR,
        MACADDR,
        MACADDR8,
        ENUM,
        ARRAY;

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
                case "time", "time without time zone" -> TIME;
                case "timetz", "time with time zone" -> TIMETZ;
                case "timestamp", "timestamp without time zone" -> TIMESTAMP;
                case "timestamptz", "timestamp with time zone" -> TIMESTAMPTZ;
                case "uuid" -> UUID;
                case "json" -> JSON;
                case "jsonb" -> JSONB;
                case "xml" -> XML;
                case "inet" -> INET;
                case "cidr" -> CIDR;
                case "macaddr" -> MACADDR;
                case "macaddr8" -> MACADDR8;
                default -> throw new IllegalArgumentException("postgres module does not support column type: " + typeName);
            };
        }

        /** pg_type oid of the type, required in the COPY BINARY array format. */
        public int getOid() {
            return switch (this) {
                case BOOL -> 16;
                case INT2 -> 21;
                case INT4 -> 23;
                case INT8 -> 20;
                case FLOAT4 -> 700;
                case FLOAT8 -> 701;
                case NUMERIC -> 1700;
                case TEXT -> 25;
                case VARCHAR -> 1043;
                case BPCHAR -> 1042;
                case BYTEA -> 17;
                case DATE -> 1082;
                case TIME -> 1083;
                case TIMETZ -> 1266;
                case TIMESTAMP -> 1114;
                case TIMESTAMPTZ -> 1184;
                case UUID -> 2950;
                case JSON -> 114;
                case JSONB -> 3802;
                case XML -> 142;
                case INET -> 869;
                case CIDR -> 650;
                case MACADDR -> 829;
                case MACADDR8 -> 774;
                // enum oids are database-specific and arrays have no single oid
                case ENUM, ARRAY -> 0;
            };
        }
    }

    public static class Column implements Serializable {

        public final String name;
        public final ColumnType type;
        // element type of an ARRAY column (null for scalar columns)
        public final ColumnType elementType;
        // element pg_type oid, required only to encode arrays (0 when unknown)
        public final int elementOid;

        public Column(final String name, final ColumnType type) {
            this(name, type, null, 0);
        }

        private Column(final String name, final ColumnType type, final ColumnType elementType, final int elementOid) {
            this.name = name;
            this.type = type;
            this.elementType = elementType;
            this.elementOid = elementOid;
        }

        public static Column arrayOf(final String name, final ColumnType elementType) {
            return arrayOf(name, elementType, elementType.getOid());
        }

        public static Column arrayOf(final String name, final ColumnType elementType, final int elementOid) {
            return new Column(name, ColumnType.ARRAY, elementType, elementOid);
        }

        @Override
        public String toString() {
            if(ColumnType.ARRAY.equals(type)) {
                return name + ":" + elementType + "[]";
            }
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

    /** A schema-qualified table name as enumerated from {@code pg_class}. */
    public record TableId(String schema, String name) implements Serializable {

        /** Unquoted {@code schema.name} form, used for pattern matching and log/error messages. */
        public String qualifiedName() {
            return schema + "." + name;
        }

        /** Quoted {@code "schema"."name"} form, safe to embed in SQL (and {@code ::regclass} casts). */
        public String quotedName() {
            return quoteIdentifier(schema) + "." + quoteIdentifier(name);
        }
    }

    public static String quoteIdentifier(final String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public static String quoteLiteral(final String literal) {
        return "'" + literal.replace("'", "''") + "'";
    }

    /**
     * Parses a table reference as written in a config ({@code table}, {@code schema.table},
     * optionally with double-quoted parts) into a {@link TableId}. A bare name lives in the
     * {@code public} schema.
     */
    public static TableId parseTableId(final String table) {
        if(table == null || table.isBlank()) {
            throw new IllegalArgumentException("table must not be empty");
        }
        final List<String> parts = new ArrayList<>();
        final StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for(int i = 0; i < table.length(); i++) {
            final char c = table.charAt(i);
            if(c == '"') {
                if(quoted && i + 1 < table.length() && table.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if(c == '.' && !quoted) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        parts.add(current.toString());
        if(parts.size() > 2 || parts.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("Illegal table reference: " + table + " (expected table or schema.table)");
        }
        return parts.size() == 1
                ? new TableId("public", parts.get(0).trim())
                : new TableId(parts.get(0).trim(), parts.get(1).trim());
    }


    // Connection handling shared by the postgres source and sink modules

    /** Resolved connection settings ({@link #resolveCredentials}). */
    public record Credentials(String url, String user, String password) implements Serializable { }

    /**
     * Resolves the connection credentials of a module: a missing user means Cloud SQL IAM
     * authentication with the worker service account, otherwise secret references in
     * {@code user} / {@code password} are resolved.
     */
    public static Credentials resolveCredentials(
            final String url,
            final String user,
            final String password,
            final PipelineOptions options) {

        if(user == null) {
            final String serviceAccount = DataflowOptions.getServiceAccount(options);
            LOG.info("Using worker service account: '{}' for database user", serviceAccount);
            final String iamUrl = url.contains("enableIamAuth") ? url : url + "&enableIamAuth=true";
            return new Credentials(iamUrl, serviceAccount.replace(".gserviceaccount.com", ""), "dummy");
        }
        if(SecretProviders.isSecretReference(user) || SecretProviders.isSecretReference(password)) {
            LOG.info("parameters.user|password is secret resource.");
            return new Credentials(url, SecretProviders.resolveIfSecret(user), SecretProviders.resolveIfSecret(password));
        }
        return new Credentials(url, user, password);
    }

    // Worker-shared connection pools, reference-counted per url+user+mode so that the teardown
    // of one DoFn does not close a pool other DoFn instances still use.
    private static final Map<String, PoolEntry> POOLS = new HashMap<>();

    private static final class PoolEntry {

        private final HikariDataSource dataSource;
        private int refCount;

        private PoolEntry(final HikariDataSource dataSource) {
            this.dataSource = dataSource;
        }
    }

    public static HikariDataSource acquirePool(
            final Credentials credentials,
            final boolean readOnly,
            final int maximumPoolSize) {

        final String key = poolKey(credentials, readOnly);
        synchronized (POOLS) {
            PoolEntry entry = POOLS.get(key);
            if (entry == null) {
                final HikariConfig config = new HikariConfig();
                config.setJdbcUrl(credentials.url());
                config.setUsername(credentials.user());
                config.setPassword(credentials.password());
                config.setDriverClassName(DRIVER);
                config.setMaximumPoolSize(maximumPoolSize);
                config.setReadOnly(readOnly);
                // the sink drives its own transactions; the source's COPY OUT must stay autocommit,
                // otherwise pgjdbc opens a BEGIN that the unwrapped connection never closes
                config.setAutoCommit(readOnly);
                // a write flush may wait for a pooled connection behind other threads' COPYs
                config.setConnectionTimeout(300_000L);
                config.addDataSourceProperty("ApplicationName", "mercari-pipeline");
                entry = new PoolEntry(new HikariDataSource(config));
                POOLS.put(key, entry);
            }
            entry.refCount++;
            return entry.dataSource;
        }
    }

    public static void releasePool(final Credentials credentials, final boolean readOnly) {
        final String key = poolKey(credentials, readOnly);
        synchronized (POOLS) {
            final PoolEntry entry = POOLS.get(key);
            if (entry == null) {
                return;
            }
            entry.refCount--;
            if (entry.refCount <= 0) {
                POOLS.remove(key);
                entry.dataSource.close();
            }
        }
    }

    private static String poolKey(final Credentials credentials, final boolean readOnly) {
        return credentials.url() + "|" + credentials.user() + "|" + (readOnly ? "ro" : "rw");
    }


    // Destination table description (sink)

    /**
     * Description of a destination table: its columns (with COPY BINARY types), primary key,
     * the column sets of its plain unique indexes (usable for {@code ON CONFLICT} inference),
     * generated columns (which COPY cannot write) and the server version.
     */
    public record TableInfo(
            TableId tableId,
            List<Column> columns,
            List<String> primaryKey,
            List<List<String>> uniqueIndexes,
            Set<String> generatedColumns,
            int serverVersion) implements Serializable {

        /** See {@link PostgresUtil#findColumn(List, String)}. */
        public Column findColumn(final String name) {
            return PostgresUtil.findColumn(columns, name);
        }

        public boolean hasUniqueIndex(final Collection<String> columnNames) {
            final Set<String> wanted = new HashSet<>(columnNames);
            return uniqueIndexes.stream().anyMatch(index -> new HashSet<>(index).equals(wanted));
        }
    }

    public static boolean tableExists(final Connection connection, final TableId tableId) throws SQLException {
        final String sql = "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = ? AND c.relkind IN ('r', 'p', 'f', 'v')";
        try(final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.name());
            try(final ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    public static int getServerVersion(final Connection connection) throws SQLException {
        try(final PreparedStatement statement = connection.prepareStatement("SHOW server_version_num");
            final ResultSet resultSet = statement.executeQuery()) {
            if(!resultSet.next()) {
                throw new SQLException("Empty result for SHOW server_version_num");
            }
            return Integer.parseInt(resultSet.getString(1));
        }
    }

    public static TableInfo getTableInfo(final Connection connection, final TableId tableId) throws SQLException {
        final List<Column> columns = getColumnsFromQuery(connection, "SELECT * FROM " + tableId.quotedName() + " WHERE false");

        final Set<String> generatedColumns = new HashSet<>();
        final String generatedSql = """
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''
                """;
        try(final PreparedStatement statement = connection.prepareStatement(generatedSql)) {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.name());
            try(final ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    generatedColumns.add(resultSet.getString(1));
                }
            }
        }

        final List<String> primaryKey = new ArrayList<>();
        final List<List<String>> uniqueIndexes = new ArrayList<>();
        // plain unique indexes only: partial or expression indexes cannot be inferred by ON CONFLICT
        final String indexSql = """
                SELECT i.indisprimary, array_agg(a.attname ORDER BY k.ord)
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
                WHERE n.nspname = ? AND c.relname = ? AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL
                GROUP BY i.indexrelid, i.indisprimary
                """;
        try(final PreparedStatement statement = connection.prepareStatement(indexSql)) {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.name());
            try(final ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    final boolean primary = resultSet.getBoolean(1);
                    final String[] names = (String[]) resultSet.getArray(2).getArray();
                    final List<String> index = List.of(names);
                    uniqueIndexes.add(index);
                    if(primary) {
                        primaryKey.addAll(index);
                    }
                }
            }
        }
        return new TableInfo(tableId, columns, primaryKey, uniqueIndexes, generatedColumns, getServerVersion(connection));
    }


    // Statement generation for the sink module

    /** Write operations of the sink: the user-facing ops plus MERGE (row-level op column). */
    public enum WriteOp {
        INSERT,
        INSERT_OR_UPDATE,
        INSERT_OR_DONOTHING,
        DELETE,
        MERGE;

        /** Whether the rows are staged in a temporary table and applied with a statement. */
        public boolean isStaged() {
            return !INSERT.equals(this);
        }
    }

    /** Name of the staging column that carries the row-level op in MERGE mode. */
    public static final String STAGING_OP_COLUMN = "_mp_op";
    public static final String STAGING_OP_DELETE = "DELETE";
    public static final String STAGING_OP_UPSERT = "UPSERT";

    /**
     * Everything the apply statement of a staged write depends on. Column names are unquoted;
     * {@code updateColumns} is the subset of {@code columns} an upsert overwrites and
     * {@code sequenceColumn} the destination column that guards MERGE against stale rows
     * (compared as text with the C collation).
     */
    public record ApplySpec(
            WriteOp op,
            TableId target,
            String staging,
            List<String> columns,
            List<String> keyColumns,
            List<String> updateColumns,
            String updateCondition,
            String sequenceColumn) implements Serializable { }

    /**
     * Exact-name match first, then the unique case-insensitive match; null if none or ambiguous.
     */
    public static Column findColumn(final List<Column> columns, final String name) {
        for(final Column column : columns) {
            if(column.name.equals(name)) {
                return column;
            }
        }
        Column found = null;
        for(final Column column : columns) {
            if(column.name.equalsIgnoreCase(name)) {
                if(found != null) {
                    return null;
                }
                found = column;
            }
        }
        return found;
    }

    /**
     * {@code CREATE TEMP TABLE IF NOT EXISTS} for the staging table: the staged columns with the
     * destination's column types (so the COPY BINARY encoding of the destination applies as is)
     * plus the op column in MERGE mode. It is created with {@code AS SELECT ... WITH NO DATA}
     * rather than {@code LIKE} so that the destination's NOT NULL constraints are not inherited:
     * a delete stages the keys only (op mode) or NULL placeholders (cdc mode).
     * {@code ON COMMIT DELETE ROWS} empties it at every commit/rollback, so one session-scoped
     * table serves every batch of the connection.
     */
    public static String createStagingTableStatement(
            final String staging,
            final TableId target,
            final List<String> columnNames,
            final boolean withOpColumn) {

        final String select = columnNames.stream().map(PostgresUtil::quoteIdentifier).collect(Collectors.joining(", "))
                + (withOpColumn ? ", NULL::text AS " + quoteIdentifier(STAGING_OP_COLUMN) : "");
        return "CREATE TEMP TABLE IF NOT EXISTS " + quoteIdentifier(staging)
                + " ON COMMIT DELETE ROWS AS SELECT " + select + " FROM " + target.quotedName() + " WITH NO DATA";
    }

    /**
     * A staging table name unique to the destination and the column layout of the writer (so a
     * layout change after a schema migration never meets a stale session table), within the
     * 63-byte identifier limit.
     */
    public static String createStagingTableName(final TableId target, final List<String> columns, final WriteOp op) {
        final String fingerprint = Integer.toHexString(
                (target.qualifiedName() + "|" + String.join(",", columns) + "|" + op).hashCode());
        final String base = ("_mp_" + target.name()).replaceAll("[^A-Za-z0-9_]", "_");
        return base.substring(0, Math.min(base.length(), 63 - fingerprint.length() - 1)) + "_" + fingerprint;
    }

    public static String createTruncateStatement(final TableId target) {
        return "TRUNCATE TABLE " + target.quotedName();
    }

    public static String createDeleteAllStatement(final TableId target) {
        return "DELETE FROM " + target.quotedName();
    }

    /** The statement that applies the staged rows to the destination ({@link ApplySpec#op()}). */
    public static String createApplyStatement(final ApplySpec spec) {
        final String target = spec.target().quotedName();
        final String staging = quoteIdentifier(spec.staging());
        final String columns = spec.columns().stream().map(PostgresUtil::quoteIdentifier).collect(Collectors.joining(", "));
        final String keys = spec.keyColumns().stream().map(PostgresUtil::quoteIdentifier).collect(Collectors.joining(", "));
        return switch (spec.op()) {
            case INSERT -> throw new IllegalArgumentException("INSERT rows are copied directly into the destination");
            case INSERT_OR_UPDATE -> {
                // the last staged row of a key wins (ctid order = COPY order); ordering by the key
                // also gives concurrent batches a consistent lock order
                final StringBuilder sb = new StringBuilder();
                // AS target: lets updateCondition refer to the existing row as target (and the new row as EXCLUDED)
                sb.append("INSERT INTO ").append(target).append(" AS target (").append(columns).append(")")
                        .append(" SELECT DISTINCT ON (").append(keys).append(") ").append(columns)
                        .append(" FROM ").append(staging)
                        .append(" ORDER BY ").append(keys).append(", ctid DESC")
                        .append(" ON CONFLICT (").append(keys).append(")");
                if(spec.updateColumns().isEmpty()) {
                    sb.append(" DO NOTHING");
                } else {
                    sb.append(" DO UPDATE SET ").append(spec.updateColumns().stream()
                            .map(c -> quoteIdentifier(c) + " = EXCLUDED." + quoteIdentifier(c))
                            .collect(Collectors.joining(", ")));
                    if(spec.updateCondition() != null && !spec.updateCondition().isBlank()) {
                        sb.append(" WHERE ").append(spec.updateCondition());
                    }
                }
                yield sb.toString();
            }
            case INSERT_OR_DONOTHING ->
                // DO NOTHING tolerates a duplicated key within the statement: the first staged row
                // (ctid order = COPY order) wins; the sort alone is not stable
                    "INSERT INTO " + target + " (" + columns + ")"
                            + " SELECT " + columns + " FROM " + staging + " ORDER BY " + keys + ", ctid"
                            + " ON CONFLICT (" + keys + ") DO NOTHING";
            case DELETE ->
                    "DELETE FROM " + target + " USING (SELECT DISTINCT " + keys + " FROM " + staging + ") AS s"
                            + " WHERE " + spec.keyColumns().stream()
                            .map(k -> target + "." + quoteIdentifier(k) + " = s." + quoteIdentifier(k))
                            .collect(Collectors.joining(" AND "));
            case MERGE -> {
                final String opColumn = quoteIdentifier(STAGING_OP_COLUMN);
                final String order = spec.sequenceColumn() != null
                        ? "s0." + quoteIdentifier(spec.sequenceColumn()) + " COLLATE \"C\" DESC"
                        : "s0.ctid DESC";
                final StringBuilder sb = new StringBuilder();
                sb.append("MERGE INTO ").append(target).append(" AS t")
                        .append(" USING (SELECT DISTINCT ON (").append(keys).append(") ").append(columns).append(", ").append(opColumn)
                        .append(" FROM ").append(staging).append(" AS s0")
                        .append(" ORDER BY ").append(keys).append(", ").append(order).append(") AS s")
                        .append(" ON ").append(spec.keyColumns().stream()
                                .map(k -> "t." + quoteIdentifier(k) + " = s." + quoteIdentifier(k))
                                .collect(Collectors.joining(" AND ")));
                if(spec.sequenceColumn() != null) {
                    final String seq = quoteIdentifier(spec.sequenceColumn());
                    sb.append(" WHEN MATCHED AND t.").append(seq).append(" IS NOT NULL AND t.").append(seq)
                            .append(" COLLATE \"C\" >= s.").append(seq).append(" COLLATE \"C\" THEN DO NOTHING");
                }
                sb.append(" WHEN MATCHED AND s.").append(opColumn).append(" = ").append(quoteLiteral(STAGING_OP_DELETE)).append(" THEN DELETE");
                if(spec.updateColumns().isEmpty()) {
                    sb.append(" WHEN MATCHED THEN DO NOTHING");
                } else {
                    sb.append(" WHEN MATCHED THEN UPDATE SET ").append(spec.updateColumns().stream()
                            .map(c -> quoteIdentifier(c) + " = s." + quoteIdentifier(c))
                            .collect(Collectors.joining(", ")));
                }
                sb.append(" WHEN NOT MATCHED AND s.").append(opColumn).append(" <> ").append(quoteLiteral(STAGING_OP_DELETE))
                        .append(" THEN INSERT (").append(columns).append(") VALUES (")
                        .append(spec.columns().stream().map(c -> "s." + quoteIdentifier(c)).collect(Collectors.joining(", ")))
                        .append(")");
                yield sb.toString();
            }
        };
    }

    /**
     * {@code CREATE TABLE IF NOT EXISTS} derived from an avro schema: nullable fields become
     * nullable columns, nested records and maps become {@code jsonb}, one-dimensional arrays
     * become array columns; {@code keyFields} form the primary key.
     */
    public static String createCreateTableStatement(
            final TableId target,
            final Schema avroSchema,
            final List<String> keyFields) {

        final List<String> definitions = new ArrayList<>();
        for(final Schema.Field field : avroSchema.getFields()) {
            final boolean key = keyFields != null && keyFields.contains(field.name());
            final String type = toColumnTypeName(field.schema());
            definitions.add(quoteIdentifier(field.name()) + " " + type
                    + (key || !AvroSchemaUtil.isNullable(field.schema()) ? " NOT NULL" : ""));
        }
        if(keyFields != null && !keyFields.isEmpty()) {
            definitions.add("PRIMARY KEY (" + keyFields.stream().map(PostgresUtil::quoteIdentifier).collect(Collectors.joining(", ")) + ")");
        }
        return "CREATE TABLE IF NOT EXISTS " + target.quotedName() + " (" + String.join(", ", definitions) + ")";
    }

    private static String toColumnTypeName(final Schema schema) {
        final Schema unnested = unnestUnion(schema);
        return switch (unnested.getType()) {
            case BOOLEAN -> "boolean";
            case INT -> {
                if(LogicalTypes.date().equals(unnested.getLogicalType())) {
                    yield "date";
                } else if(LogicalTypes.timeMillis().equals(unnested.getLogicalType())) {
                    yield "time";
                }
                yield "integer";
            }
            case LONG -> {
                if(LogicalTypes.timestampMillis().equals(unnested.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(unnested.getLogicalType())) {
                    yield "timestamptz";
                } else if(LogicalTypes.timeMicros().equals(unnested.getLogicalType())) {
                    yield "time";
                }
                yield "bigint";
            }
            case FLOAT -> "real";
            case DOUBLE -> "double precision";
            case STRING -> {
                if(AvroSchemaUtil.isSqlTypeJson(unnested)) {
                    yield "jsonb";
                }
                yield "text";
            }
            case ENUM -> "text";
            case BYTES, FIXED -> {
                if(unnested.getLogicalType() instanceof LogicalTypes.Decimal decimal) {
                    yield "numeric(" + decimal.getPrecision() + ", " + decimal.getScale() + ")";
                }
                yield "bytea";
            }
            case RECORD, MAP -> "jsonb";
            case ARRAY -> {
                final Schema element = unnestUnion(unnested.getElementType());
                if(Schema.Type.RECORD.equals(element.getType()) || Schema.Type.MAP.equals(element.getType())
                        || Schema.Type.ARRAY.equals(element.getType())) {
                    yield "jsonb";
                }
                yield toColumnTypeName(element) + "[]";
            }
            default -> throw new IllegalArgumentException("postgres module does not support creating a column for avro type: " + unnested);
        };
    }


    // JSON value conversion (cdc apply mode: envelope keys/after values to encodable values)

    /**
     * Converts a change record JSON value into a value {@link #write} accepts for the column
     * (the JSON forms produced by the cdc transform providers: ISO-8601 or database text form
     * dates/times ({@code 2023-11-14 22:13:20} as emitted by canal-json), base64 bytes, plain
     * decimal strings, arrays of the above).
     */
    public static Object fromJsonValue(final Column column, final JsonElement json) {
        if(json == null || json.isJsonNull()) {
            return null;
        }
        if(ColumnType.ARRAY.equals(column.type)) {
            if(!json.isJsonArray()) {
                throw new IllegalArgumentException("Failed to convert value: " + json + " to array column: " + column.name);
            }
            final List<Object> list = new ArrayList<>();
            for(final JsonElement element : json.getAsJsonArray()) {
                list.add(fromJsonScalar(column.elementType, element));
            }
            return list;
        }
        return fromJsonScalar(column.type, json);
    }

    private static Object fromJsonScalar(final ColumnType type, final JsonElement json) {
        if(json.isJsonNull()) {
            return null;
        }
        return switch (type) {
            case BOOL -> json.isJsonPrimitive() && json.getAsJsonPrimitive().isBoolean()
                    ? json.getAsBoolean() : Boolean.parseBoolean(json.getAsString());
            case INT2, INT4, INT8 -> json.isJsonPrimitive() && json.getAsJsonPrimitive().isNumber()
                    ? json.getAsBigDecimal().longValueExact() : Long.parseLong(json.getAsString());
            case FLOAT4, FLOAT8 -> json.getAsDouble();
            case NUMERIC -> json.getAsBigDecimal();
            case BYTEA -> Base64.getDecoder().decode(json.getAsString());
            case DATE -> DateTimeUtil.toEpochDay(json.getAsString());
            case TIME, TIMETZ -> DateTimeUtil.toMicroOfDay(json.getAsString());
            case TIMESTAMP, TIMESTAMPTZ -> parseTimestampMicros(json.getAsString());
            case JSON, JSONB -> json.isJsonPrimitive() && json.getAsJsonPrimitive().isString()
                    ? json.getAsString() : json.toString();
            case TEXT, VARCHAR, BPCHAR, UUID, XML, ENUM, INET, CIDR, MACADDR, MACADDR8 ->
                    json.isJsonPrimitive() ? json.getAsString() : json.toString();
            case ARRAY -> throw new IllegalStateException("array must not be an array element type");
        };
    }

    /**
     * Parses a timestamp text: ISO-8601 or the database text form ({@code T} or space
     * separator), with or without a zone (a zoneless value is UTC).
     */
    public static long parseTimestampMicros(final String text) {
        final Long micros = DateTimeUtil.toEpochMicroSecond(text);
        if(micros == null) {
            throw new IllegalArgumentException("Failed to parse timestamp: " + text);
        }
        return micros;
    }

    /**
     * Lists the base tables of the connected database: regular tables and partitioned-table
     * parents in user schemas. Leaf partitions are excluded (they are read via their parent),
     * as are the {@code information_schema} and {@code pg_*} system schemas.
     */
    public static List<TableId> getBaseTables(final Connection connection) throws SQLException {
        final String sql = """
                SELECT n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relkind IN ('r', 'p')
                  AND NOT c.relispartition
                  AND n.nspname <> 'information_schema'
                  AND n.nspname NOT LIKE 'pg\\_%'
                ORDER BY n.nspname, c.relname
                """;
        final List<TableId> tables = new ArrayList<>();
        try(final PreparedStatement statement = connection.prepareStatement(sql);
            final ResultSet resultSet = statement.executeQuery()) {
            while(resultSet.next()) {
                tables.add(new TableId(resultSet.getString(1), resultSet.getString(2)));
            }
        }
        return tables;
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
            return getColumns(connection, meta);
        }
    }

    public static List<Column> getColumns(final Connection connection, final ResultSetMetaData meta) throws SQLException {
        final List<Column> columns = new ArrayList<>();
        for(int column = 1; column <= meta.getColumnCount(); column++) {
            final String name = meta.getColumnName(column);
            final String typeName = meta.getColumnTypeName(column);
            if(java.sql.Types.ARRAY == meta.getColumnType(column) || typeName.startsWith("_")) {
                // pg array type names carry a leading underscore (e.g. _int4), possibly schema-qualified
                final String elementTypeName = typeName.replaceFirst("_", "");
                final ResolvedType elementType = resolveType(connection, elementTypeName);
                columns.add(Column.arrayOf(name, elementType.type, elementType.oid));
            } else {
                columns.add(new Column(name, resolveType(connection, typeName).type));
            }
        }
        return columns;
    }

    private record ResolvedType(ColumnType type, int oid) implements Serializable { }

    /**
     * Resolves a type name to a {@link ColumnType}. Names not covered by the built-in
     * mapping are looked up in {@code pg_type}: enum types map to {@link ColumnType#ENUM}
     * (COPY BINARY transfers enum values as their text labels) and domain types resolve
     * recursively to their base type.
     */
    private static ResolvedType resolveType(final Connection connection, final String typeName) throws SQLException {
        try {
            final ColumnType type = ColumnType.of(typeName);
            return new ResolvedType(type, type.getOid());
        } catch (final IllegalArgumentException e) {
            final String sql = "SELECT oid, typtype, typbasetype::regtype::text FROM pg_type WHERE oid = to_regtype(?)";
            try(final PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, typeName);
                try(final ResultSet resultSet = statement.executeQuery()) {
                    if(resultSet.next()) {
                        final String typtype = resultSet.getString(2);
                        if("e".equals(typtype)) {
                            return new ResolvedType(ColumnType.ENUM, resultSet.getInt(1));
                        } else if("d".equals(typtype)) {
                            return resolveType(connection, resultSet.getString(3));
                        }
                    }
                }
            }
            throw e;
        }
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
            record.put(i, decodeValue(columns.get(i), bytes));
        }
        return record;
    }

    /**
     * Decodes one binary-format field value (the format shared by COPY BINARY fields and
     * pgoutput binary-mode tuple data) into its avro-convention value.
     */
    public static Object decodeValue(final Column column, final byte[] bytes) {
        if(ColumnType.ARRAY.equals(column.type)) {
            return decodeArray(column, bytes);
        }
        return decodeScalar(column.type, bytes);
    }

    /** Converts a PostgreSQL-epoch (2000-01-01) microsecond timestamp to unix-epoch micros. */
    public static long toUnixMicros(final long postgresMicros) {
        return postgresMicros + POSTGRES_EPOCH_MICROS;
    }

    private static Object decodeScalar(final ColumnType type, final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return switch (type) {
            case BOOL -> bytes[0] != 0;
            case INT2 -> (int) buffer.getShort();
            case INT4 -> buffer.getInt();
            case INT8 -> buffer.getLong();
            case FLOAT4 -> buffer.getFloat();
            case FLOAT8 -> buffer.getDouble();
            case NUMERIC -> toAvroDecimalBytes(decodeNumeric(buffer));
            // enum values and xml documents are transferred as their text representation
            case TEXT, VARCHAR, BPCHAR, JSON, XML, ENUM -> new String(bytes, StandardCharsets.UTF_8);
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
            case TIMETZ -> {
                final long micros = buffer.getLong();
                final int offsetSeconds = buffer.getInt(); // positive west of UTC
                yield Math.floorMod(micros + offsetSeconds * 1_000_000L, MICROS_PER_DAY);
            }
            case TIMESTAMP, TIMESTAMPTZ -> buffer.getLong() + POSTGRES_EPOCH_MICROS;
            case UUID -> new UUID(buffer.getLong(), buffer.getLong()).toString();
            case INET, CIDR -> decodeInet(buffer);
            case MACADDR, MACADDR8 -> decodeMacaddr(bytes);
            case ARRAY -> throw new IllegalStateException("array must not be an array element type");
        };
    }

    private static List<Object> decodeArray(final Column column, final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final int ndim = buffer.getInt();
        buffer.getInt(); // hasnull flag
        buffer.getInt(); // element type oid
        final List<Object> list = new ArrayList<>();
        if(ndim == 0) {
            return list;
        }
        if(ndim > 1) {
            throw new IllegalStateException(
                    "postgres module does not support multidimensional array column: " + column.name + " (ndim: " + ndim + ")");
        }
        final int size = buffer.getInt();
        buffer.getInt(); // lower bound
        for(int i = 0; i < size; i++) {
            final int length = buffer.getInt();
            if(length == -1) {
                // skip null elements (consistent with ResultSetToRecordConverter)
                continue;
            }
            final byte[] elementBytes = new byte[length];
            buffer.get(elementBytes);
            list.add(decodeScalar(column.elementType, elementBytes));
        }
        return list;
    }

    private static String decodeInet(final ByteBuffer buffer) {
        buffer.get(); // address family (PGSQL_AF_INET or PGSQL_AF_INET6)
        final int bits = buffer.get() & 0xFF;
        buffer.get(); // is_cidr flag
        final int length = buffer.get() & 0xFF;
        final byte[] address = new byte[length];
        buffer.get(address);
        try {
            // always append the netmask suffix, matching the postgres inet/cidr text output
            return InetAddress.getByAddress(address).getHostAddress() + "/" + bits;
        } catch (final UnknownHostException e) {
            throw new IllegalStateException("Illegal inet binary length: " + length, e);
        }
    }

    private static String decodeMacaddr(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for(final byte b : bytes) {
            if(!sb.isEmpty()) {
                sb.append(':');
            }
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
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

        final Object[] values = new Object[columns.size()];
        final List<Schema> fieldSchemas = new ArrayList<>(columns.size());
        for(int i = 0; i < columns.size(); i++) {
            final Schema.Field field = fields.get(i);
            values[i] = field == null ? null : record.get(field.pos());
            fieldSchemas.add(field == null ? null : field.schema());
        }
        writeValues(output, columns, fieldSchemas, values);
    }

    /**
     * Writes one tuple from already-typed values aligned with {@code columns} by index
     * (the cdc apply path: values converted with {@link #fromJsonValue}, no avro schema).
     */
    public static void writeValues(
            final DataOutputStream output,
            final List<Column> columns,
            final Object[] values) throws IOException {

        writeValues(output, columns, null, values);
    }

    /**
     * Writes one tuple from values aligned with {@code columns} by index. {@code fieldSchemas}
     * (optional, may be shorter than {@code columns} and contain null entries) supplies the avro
     * field schema of a value where one is known (decimal scale, millis logical types).
     */
    public static void writeValues(
            final DataOutputStream output,
            final List<Column> columns,
            final List<Schema> fieldSchemas,
            final Object[] values) throws IOException {

        output.writeShort(columns.size());
        for(int i = 0; i < columns.size(); i++) {
            final Object value = values[i];
            if(value == null) {
                output.writeInt(-1);
                continue;
            }
            final Schema fieldSchema = fieldSchemas != null && i < fieldSchemas.size() ? fieldSchemas.get(i) : null;
            encodeValue(output, columns.get(i), unnestUnion(fieldSchema), value);
        }
    }

    private static void encodeValue(
            final DataOutputStream output,
            final Column column,
            final Schema fieldSchema,
            final Object value) throws IOException {

        if(ColumnType.ARRAY.equals(column.type)) {
            encodeArray(output, column, fieldSchema, value);
            return;
        }
        encodeScalar(output, column.type, fieldSchema, value);
    }

    private static void encodeArray(
            final DataOutputStream output,
            final Column column,
            final Schema fieldSchema,
            final Object value) throws IOException {

        if(column.elementOid <= 0) {
            throw new IllegalStateException("element type oid is required to encode array column: " + column.name);
        }
        if(!(value instanceof Collection<?> collection)) {
            throw new IllegalArgumentException("Failed to convert value: " + value + " to array");
        }
        final Schema elementSchema;
        if(fieldSchema != null && Schema.Type.ARRAY.equals(fieldSchema.getType())) {
            elementSchema = unnestUnion(fieldSchema.getElementType());
        } else {
            elementSchema = null;
        }
        boolean hasNull = false;
        for(final Object element : collection) {
            if(element == null) {
                hasNull = true;
                break;
            }
        }
        final ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
        try(final DataOutputStream payload = new DataOutputStream(payloadBytes)) {
            payload.writeInt(collection.isEmpty() ? 0 : 1); // ndim
            payload.writeInt(hasNull ? 1 : 0);
            payload.writeInt(column.elementOid);
            if(!collection.isEmpty()) {
                payload.writeInt(collection.size());
                payload.writeInt(1); // lower bound
                for(final Object element : collection) {
                    if(element == null) {
                        payload.writeInt(-1);
                    } else {
                        encodeScalar(payload, column.elementType, elementSchema, element);
                    }
                }
            }
        }
        output.writeInt(payloadBytes.size());
        payloadBytes.writeTo(output);
    }

    private static void encodeScalar(
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
                final long l = toLong(value);
                if(l < Short.MIN_VALUE || l > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("Value: " + value + " is out of range for smallint column");
                }
                output.writeInt(2);
                output.writeShort((short) l);
            }
            case INT4 -> {
                final long l = toLong(value);
                if(l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Value: " + value + " is out of range for integer column");
                }
                output.writeInt(4);
                output.writeInt((int) l);
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
            case TEXT, VARCHAR, BPCHAR, JSON, XML, ENUM -> {
                final byte[] bytes = toText(value).getBytes(StandardCharsets.UTF_8);
                output.writeInt(bytes.length);
                output.write(bytes);
            }
            case JSONB -> {
                final byte[] bytes = toText(value).getBytes(StandardCharsets.UTF_8);
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
            case TIMETZ -> {
                output.writeInt(12);
                output.writeLong(toMicroOfDay(fieldSchema, value));
                output.writeInt(0); // zone offset in seconds (values are normalized to UTC)
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
            case INET, CIDR -> {
                final byte[] bytes = encodeInet(value.toString(), ColumnType.CIDR.equals(type));
                output.writeInt(bytes.length);
                output.write(bytes);
            }
            case MACADDR -> {
                final byte[] bytes = encodeMacaddr(value.toString(), 6);
                output.writeInt(bytes.length);
                output.write(bytes);
            }
            case MACADDR8 -> {
                final byte[] bytes = encodeMacaddr(value.toString(), 8);
                output.writeInt(bytes.length);
                output.write(bytes);
            }
            case ARRAY -> throw new IllegalStateException("array must not be an array element type");
        }
    }

    private static byte[] encodeInet(final String text, final boolean cidr) {
        final String trimmed = text.trim();
        final int slash = trimmed.indexOf('/');
        final String host = slash < 0 ? trimmed : trimmed.substring(0, slash);
        final byte[] address = InetAddresses.forString(host).getAddress();
        final int bits = slash < 0 ? address.length * 8 : Integer.parseInt(trimmed.substring(slash + 1));
        final ByteBuffer buffer = ByteBuffer.allocate(4 + address.length);
        buffer.put((byte) (address.length == 4 ? PGSQL_AF_INET : PGSQL_AF_INET6));
        buffer.put((byte) bits);
        buffer.put((byte) (cidr ? 1 : 0));
        buffer.put((byte) address.length);
        buffer.put(address);
        return buffer.array();
    }

    private static byte[] encodeMacaddr(final String text, final int length) {
        final String[] parts = text.trim().split("[:-]");
        if(parts.length != length) {
            throw new IllegalArgumentException("Failed to convert value: " + text + " to macaddr" + (length == 8 ? "8" : ""));
        }
        final byte[] bytes = new byte[length];
        for(int i = 0; i < length; i++) {
            bytes[i] = (byte) Integer.parseInt(parts[i], 16);
        }
        return bytes;
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

    /** Text form of a value for text-like columns: structured values (records, maps, lists) become JSON. */
    private static String toText(final Object value) {
        return switch (value) {
            case CharSequence s -> s.toString();
            case GenericEnumSymbol<?> e -> e.toString();
            case GenericRecord r -> AvroToJsonConverter.convertObject(r).toString();
            case Map<?, ?> m -> toJsonElement(m).toString();
            case Collection<?> c -> toJsonElement(c).toString();
            case ByteBuffer b -> Base64.getEncoder().encodeToString(toBytes(b));
            case byte[] b -> Base64.getEncoder().encodeToString(b);
            default -> value.toString();
        };
    }

    private static JsonElement toJsonElement(final Object value) {
        return switch (value) {
            case null -> JsonNull.INSTANCE;
            case GenericRecord r -> AvroToJsonConverter.convertObject(r);
            case Map<?, ?> m -> {
                final JsonObject object = new JsonObject();
                for(final Map.Entry<?, ?> entry : m.entrySet()) {
                    object.add(entry.getKey().toString(), toJsonElement(entry.getValue()));
                }
                yield object;
            }
            case Collection<?> c -> {
                final JsonArray array = new JsonArray();
                for(final Object element : c) {
                    array.add(toJsonElement(element));
                }
                yield array;
            }
            case Boolean b -> new JsonPrimitive(b);
            case Number n -> new JsonPrimitive(n);
            case GenericFixed f -> new JsonPrimitive(Base64.getEncoder().encodeToString(f.bytes()));
            case ByteBuffer b -> new JsonPrimitive(Base64.getEncoder().encodeToString(toBytes(b)));
            case byte[] b -> new JsonPrimitive(Base64.getEncoder().encodeToString(b));
            default -> new JsonPrimitive(value.toString());
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