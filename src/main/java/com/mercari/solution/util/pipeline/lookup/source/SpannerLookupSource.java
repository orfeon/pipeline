package com.mercari.solution.util.pipeline.lookup.source;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.cloud.google.SpannerUtil;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.pipeline.lookup.PerKeyLookup;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Cloud Spanner-backed {@link LookupSource} exposing two kinds of tables:
 *
 * <p><b>Native tables</b> use the client's key-driven
 * {@code read(table, KeySet, columns)} — the natural fit for the lookup-join:
 * point keys become {@link Key}s and prefix+range requests become
 * {@link KeyRange}s, both in one {@link KeySet}. Unique secondary indexes are
 * answered with a two-step {@code readUsingIndex} (index → base primary keys →
 * full rows) inside one read-only snapshot. Table metadata (columns, primary
 * key, unique indexes) is derived once from {@code INFORMATION_SCHEMA} — at
 * pipeline construction time — and serialized with the source. Columns of
 * unsupported types (ARRAY / STRUCT / TOKENLIST) are skipped with a warning.
 *
 * <p><b>Query tables</b> expose a caller-supplied <em>parameterized GoogleSQL or
 * GQL statement</em> as a synthetic key-driven table — the generic "input key →
 * parameterized query → rows" shape behind Spanner Graph traversals and
 * full-text search. The statement binds one parameter and returns the join-key
 * column; two binding strategies cover the two backend shapes
 * ({@link BindMode}): {@code ARRAY} binds the batch's distinct keys to one
 * array parameter and runs the statement once per batch (graph's
 * {@code WHERE n.id IN UNNEST(@keys)}), {@code PER_KEY} runs it once per
 * distinct key binding a scalar (search's {@code SEARCH(tokens, @query)},
 * which has no array form). The statement never goes through Calcite. The
 * result schema is given explicitly or derived by dry-running the statement
 * through {@code analyzeQuery(PLAN)}. Only point equality on the (single) key
 * column is supported.
 */
public class SpannerLookupSource extends LookupSource {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerLookupSource.class);

    /** How a query table binds the lookup keys to its statement parameter. */
    public enum BindMode {
        /** Bind the batch's distinct keys to one array param; run the statement once. */
        ARRAY,
        /** Run the statement once per distinct key, binding a scalar param. */
        PER_KEY
    }

    private record TableConfig(String name, String table, QueryTableConfig query)
            implements Serializable {
    }

    /** Configuration of one query table (a parameterized GoogleSQL/GQL statement). */
    public static class QueryTableConfig implements Serializable {

        private final String name;
        private final String sql;
        private final String keyField;
        private final String paramName;
        private final BindMode bindMode;
        private final Schema fields;   // null = derive via analyzeQuery(PLAN)

        private QueryTableConfig(QueryTableBuilder builder) {
            this.name = Objects.requireNonNull(builder.name, "query table name must not be null");
            this.sql = Objects.requireNonNull(builder.sql, "query table sql must not be null");
            this.keyField = Objects.requireNonNull(builder.keyField,
                    "query table keyField must not be null");
            this.paramName = builder.paramName;
            this.bindMode = builder.bindMode;
            this.fields = builder.fields;
            if (!sql.contains("@" + paramName)) {
                throw new IllegalArgumentException("query of spanner query table '" + name
                        + "' must reference the bind parameter '@" + paramName + "'");
            }
            if (fields != null && fieldIndex(fields, keyField) < 0) {
                throw new IllegalArgumentException("fields of spanner query table '" + name
                        + "' must contain the key field '" + keyField + "'");
            }
        }
    }

    public static QueryTableBuilder queryTable() {
        return new QueryTableBuilder();
    }

    public static class QueryTableBuilder {

        private String name;
        private String sql;
        private String keyField;
        private String paramName = "keys";
        private BindMode bindMode = BindMode.ARRAY;
        private Schema fields;

        public QueryTableBuilder withName(String name) {
            this.name = name;
            return this;
        }

        /** The parameterized GoogleSQL/GQL statement; must reference {@code @<paramName>}. */
        public QueryTableBuilder withSql(String sql) {
            this.sql = sql;
            return this;
        }

        /** The join-key column; the statement must return it (so rows join back). */
        public QueryTableBuilder withKeyField(String keyField) {
            this.keyField = keyField;
            return this;
        }

        public QueryTableBuilder withParamName(String paramName) {
            this.paramName = paramName;
            return this;
        }

        public QueryTableBuilder withBindMode(BindMode bindMode) {
            this.bindMode = bindMode;
            return this;
        }

        /** Result columns; omit to derive them via {@code analyzeQuery(PLAN)}. */
        public QueryTableBuilder withFields(List<Schema.Field> fields) {
            this.fields = Schema.of(fields);
            return this;
        }

        public QueryTableConfig build() {
            return new QueryTableConfig(this);
        }
    }

    /**
     * Serializable per-table metadata: derived from INFORMATION_SCHEMA for
     * native tables, from the config / {@code analyzeQuery} for query tables.
     */
    private static class TableMeta implements Serializable {

        private final String physicalTable;    // null for query tables
        private final QueryTableConfig query;  // null for native tables
        private final Schema schema;
        private final List<String> spannerTypes;  // base type per column (INT64, STRING, ...)
        private final List<LookupKey> keyCandidates;
        private final List<String> primaryKey;

        private TableMeta(String physicalTable, QueryTableConfig query, Schema schema,
                List<String> spannerTypes, List<LookupKey> keyCandidates,
                List<String> primaryKey) {
            this.physicalTable = physicalTable;
            this.query = query;
            this.schema = schema;
            this.spannerTypes = spannerTypes;
            this.keyCandidates = keyCandidates;
            this.primaryKey = primaryKey;
        }

        private int columnIndex(String name) {
            final int index = fieldIndex(schema, name);
            if (index < 0) {
                throw new IllegalStateException("unknown column: " + name);
            }
            return index;
        }

        private String spannerType(String name) {
            return spannerTypes.get(columnIndex(name));
        }
    }

    private static int fieldIndex(Schema schema, String name) {
        for (int i = 0; i < schema.countFields(); i++) {
            if (schema.getField(i).getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private final boolean emulator;
    private final long maxStalenessSeconds;
    private final List<TableConfig> tables;

    // Derived once (at pipeline construction) and serialized to workers.
    private Map<String, TableMeta> meta;

    private transient Spanner spanner;
    private transient DatabaseClient client;

    private SpannerLookupSource(Builder builder) {
        super(builder.name);
        this.projectId = Objects.requireNonNull(builder.projectId, "projectId must not be null");
        this.instanceId = Objects.requireNonNull(builder.instanceId, "instanceId must not be null");
        this.databaseId = Objects.requireNonNull(builder.databaseId, "databaseId must not be null");
        this.emulator = builder.emulator;
        this.maxStalenessSeconds = builder.maxStalenessSeconds;
        this.tables = List.copyOf(builder.tables);
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("spanner lookup source requires at least one table");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private String projectId;
        private String instanceId;
        private String databaseId;
        private boolean emulator;
        private long maxStalenessSeconds;
        private final List<TableConfig> tables = new ArrayList<>();

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder withInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder withDatabaseId(String databaseId) {
            this.databaseId = databaseId;
            return this;
        }

        public Builder withEmulator(boolean emulator) {
            this.emulator = emulator;
            return this;
        }

        /** Allows stale reads up to the given bound (0 = strong reads). */
        public Builder withMaxStalenessSeconds(long maxStalenessSeconds) {
            this.maxStalenessSeconds = maxStalenessSeconds;
            return this;
        }

        public Builder withTable(String name) {
            return withTable(name, name);
        }

        public Builder withTable(String name, String table) {
            this.tables.add(new TableConfig(name, table, null));
            return this;
        }

        /** Exposes a parameterized GoogleSQL/GQL statement as a key-driven table. */
        public Builder withQueryTable(QueryTableConfig query) {
            this.tables.add(new TableConfig(query.name, null, query));
            return this;
        }

        public SpannerLookupSource build() {
            return new SpannerLookupSource(this);
        }
    }

    @Override
    protected void setupInternal() {
        if (client == null) {
            this.spanner = SpannerUtil.connectSpanner(projectId, 1, 1, 4, false, emulator);
            this.client = spanner.getDatabaseClient(
                    DatabaseId.of(projectId, instanceId, databaseId));
        }
        if (meta == null) {
            final Map<String, TableMeta> derived = new LinkedHashMap<>();
            for (final TableConfig table : tables) {
                derived.put(table.name(), table.query() != null
                        ? deriveQueryTableMeta(table.query())
                        : deriveTableMeta(table));
            }
            this.meta = derived;
        }
    }

    @Override
    protected void closeInternal() {
        if (spanner != null) {
            try {
                spanner.close();
            } finally {
                spanner = null;
                client = null;
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
        // Prefix-only equality becomes a prefix KeyRange over the ordered key.
        return true;
    }

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        final TableMeta tableMeta = requireMeta(table);
        if (tableMeta.query != null) {
            return queryLookup(tableMeta, batch, projects);
        }
        final List<String> keyColumns = keyColumns(tableMeta, indexName);
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(tableMeta.schema.countFields());
        final List<String> columns = new ArrayList<>(outCols.length);
        final List<String> columnTypes = new ArrayList<>(outCols.length);
        for (final int col : outCols) {
            columns.add(tableMeta.schema.getField(col).getName());
            columnTypes.add(tableMeta.spannerTypes.get(col));
        }
        final KeySet keySet = buildKeySet(tableMeta, keyColumns, batch);

        return indexName == null
                ? readByKey(tableMeta, keySet, columns, columnTypes)
                : readByIndex(tableMeta, indexName, keySet, columns, columnTypes);
    }

    /** Primary-key read: read(table, KeySet, columns). */
    private List<Object[]> readByKey(TableMeta tableMeta, KeySet keySet, List<String> columns,
            List<String> columnTypes) {
        final List<Object[]> rows = new ArrayList<>();
        try (ReadContext context = singleUse();
             ResultSet rs = context.read(tableMeta.physicalTable, keySet, columns)) {
            while (rs.next()) {
                rows.add(decodeRow(rs, columnTypes));
            }
        }
        return rows;
    }

    /**
     * Unique-index read (two-step): readUsingIndex to get the base primary keys of
     * matching rows, then read the full rows by those keys — both in one read-only
     * snapshot ({@code readUsingIndex} can only return index/PK/stored columns).
     */
    private List<Object[]> readByIndex(TableMeta tableMeta, String indexName, KeySet indexKeySet,
            List<String> columns, List<String> columnTypes) {
        final List<String> basePk = tableMeta.primaryKey;
        final List<Object[]> rows = new ArrayList<>();
        try (ReadOnlyTransaction tx = readOnlyTransaction()) {
            final KeySet.Builder pkBuilder = KeySet.newBuilder();
            final Map<String, Boolean> seen = new LinkedHashMap<>();
            try (ResultSet rs = tx.readUsingIndex(
                    tableMeta.physicalTable, indexName, indexKeySet, basePk)) {
                while (rs.next()) {
                    final Object[] pkValues = new Object[basePk.size()];
                    for (int i = 0; i < basePk.size(); i++) {
                        final String type = tableMeta.spannerType(basePk.get(i));
                        pkValues[i] = toKey(decode(rs, i, type), type);
                    }
                    final Key pk = Key.of(pkValues);
                    if (seen.put(pk.toString(), Boolean.TRUE) == null) {
                        pkBuilder.addKey(pk);
                    }
                }
            }
            if (seen.isEmpty()) {
                return rows;
            }
            try (ResultSet rs = tx.read(tableMeta.physicalTable, pkBuilder.build(), columns)) {
                while (rs.next()) {
                    rows.add(decodeRow(rs, columnTypes));
                }
            }
        }
        return rows;
    }

    private Object[] decodeRow(ResultSet rs, List<String> columnTypes) {
        final Object[] row = new Object[columnTypes.size()];
        for (int i = 0; i < columnTypes.size(); i++) {
            row[i] = decode(rs, i, columnTypes.get(i));
        }
        return row;
    }

    private ReadContext singleUse() {
        return maxStalenessSeconds > 0
                ? client.singleUse(TimestampBound.ofMaxStaleness(
                        maxStalenessSeconds, TimeUnit.SECONDS))
                : client.singleUse();
    }

    private ReadOnlyTransaction readOnlyTransaction() {
        return maxStalenessSeconds > 0
                ? client.readOnlyTransaction(TimestampBound.ofMaxStaleness(
                        maxStalenessSeconds, TimeUnit.SECONDS))
                : client.readOnlyTransaction();
    }

    private KeySet buildKeySet(TableMeta tableMeta, List<String> keyColumns, LookupBatch batch) {
        final int prefixLen = batch.prefixLength();
        final KeySet.Builder builder = KeySet.newBuilder();
        final Map<String, Boolean> seen = new LinkedHashMap<>();
        for (final LookupRequest request : batch.requests()) {
            if (request.isRange()) {
                final Object[] start = new Object[prefixLen + 1];
                final Object[] end = new Object[prefixLen + 1];
                for (int i = 0; i < prefixLen; i++) {
                    final Object key = toKey(request.prefix().get(i),
                            tableMeta.spannerType(keyColumns.get(i)));
                    start[i] = key;
                    end[i] = key;
                }
                final String rangeType = tableMeta.spannerType(keyColumns.get(prefixLen));
                start[prefixLen] = toKey(request.lower(), rangeType);
                end[prefixLen] = toKey(request.upper(), rangeType);
                final Key lo = Key.of(start);
                final Key hi = Key.of(end);
                if (seen.put("r:" + lo + ".." + hi, Boolean.TRUE) == null) {
                    builder.addRange(range(lo, hi,
                            request.lowerInclusive(), request.upperInclusive()));
                }
            } else {
                final Object[] values = new Object[request.prefix().size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = toKey(request.prefix().get(i),
                            tableMeta.spannerType(keyColumns.get(i)));
                }
                final Key key = Key.of(values);
                if (request.prefix().size() < keyColumns.size()) {
                    // Prefix-only equality: a partial key matches all rows under
                    // the prefix as a closed-closed KeyRange.
                    if (seen.put("p:" + key, Boolean.TRUE) == null) {
                        builder.addRange(KeyRange.closedClosed(key, key));
                    }
                } else if (seen.put("k:" + key, Boolean.TRUE) == null) {
                    builder.addKey(key);
                }
            }
        }
        return builder.build();
    }

    private static KeyRange range(Key lo, Key hi, boolean loInc, boolean hiInc) {
        if (loInc) {
            return hiInc ? KeyRange.closedClosed(lo, hi) : KeyRange.closedOpen(lo, hi);
        }
        return hiInc ? KeyRange.openClosed(lo, hi) : KeyRange.openOpen(lo, hi);
    }

    /** Calcite-internal value → a value acceptable to {@link Key.Builder#appendObject}. */
    private static Object toKey(Object value, String spannerType) {
        if (value == null) {
            return null;
        }
        return switch (spannerType) {
            case "STRING" -> value.toString();
            case "INT64", "ENUM" -> ((Number) value).longValue();
            case "FLOAT64" -> ((Number) value).doubleValue();
            case "FLOAT32" -> ((Number) value).floatValue();
            case "BOOL" -> value;
            case "NUMERIC" -> value;
            case "DATE" -> {
                final LocalDate ld = LocalDate.ofEpochDay(((Number) value).longValue());
                yield com.google.cloud.Date.fromYearMonthDay(
                        ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
            }
            case "TIMESTAMP" -> com.google.cloud.Timestamp.ofTimeMicroseconds(
                    ((Number) value).longValue() * 1000L);
            case "BYTES" -> ByteArray.copyFrom(((ByteString) value).getBytes());
            default -> throw new IllegalStateException("Unsupported Spanner key type: " + spannerType);
        };
    }

    /** Spanner column value (0-based) → Calcite-internal value. */
    private static Object decode(ResultSet rs, int col, String spannerType) {
        if (rs.isNull(col)) {
            return null;
        }
        return switch (spannerType) {
            case "STRING" -> rs.getString(col);
            case "INT64", "ENUM" -> rs.getLong(col);
            case "FLOAT64" -> rs.getDouble(col);
            case "FLOAT32" -> (double) rs.getFloat(col);
            case "BOOL" -> rs.getBoolean(col);
            case "NUMERIC" -> rs.getBigDecimal(col);
            case "JSON" -> rs.getJson(col);
            case "DATE" -> {
                final com.google.cloud.Date d = rs.getDate(col);
                yield (int) LocalDate.of(d.getYear(), d.getMonth(), d.getDayOfMonth()).toEpochDay();
            }
            case "TIMESTAMP" -> rs.getTimestamp(col).toSqlTimestamp().getTime();
            case "BYTES" -> new ByteString(rs.getBytes(col).toByteArray());
            default -> rs.getValue(col).toString();
        };
    }

    private TableMeta deriveTableMeta(TableConfig table) {
        final Schema.Builder schemaBuilder = Schema.builder();
        final List<String> spannerTypes = new ArrayList<>();
        try (ResultSet rs = client.singleUse().executeQuery(Statement
                .newBuilder("""
                        SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table
                        ORDER BY ORDINAL_POSITION
                        """)
                .bind("table").to(table.table())
                .build())) {
            while (rs.next()) {
                final String column = rs.getString(0);
                final String spannerType = rs.getString(1);
                final String baseType = baseType(spannerType);
                final Schema.FieldType fieldType = toFieldType(baseType);
                if (fieldType == null) {
                    LOG.warn("skipping unsupported column {}.{} of spanner type {}",
                            table.table(), column, spannerType);
                    continue;
                }
                schemaBuilder.withField(column, fieldType);
                spannerTypes.add(baseType);
            }
        }
        if (spannerTypes.isEmpty()) {
            throw new IllegalStateException(
                    "Spanner table not found for lookup: " + table.table());
        }

        final List<String> primaryKey = new ArrayList<>();
        try (ResultSet rs = client.singleUse().executeQuery(Statement
                .newBuilder("""
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS
                        WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table
                          AND INDEX_TYPE = 'PRIMARY_KEY'
                        ORDER BY ORDINAL_POSITION
                        """)
                .bind("table").to(table.table())
                .build())) {
            while (rs.next()) {
                primaryKey.add(rs.getString(0));
            }
        }

        final Map<String, List<String>> uniqueIndexes = new LinkedHashMap<>();
        try (ResultSet rs = client.singleUse().executeQuery(Statement
                .newBuilder("""
                        SELECT ic.INDEX_NAME, ic.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.INDEXES i
                        JOIN INFORMATION_SCHEMA.INDEX_COLUMNS ic
                          ON i.TABLE_SCHEMA = ic.TABLE_SCHEMA
                          AND i.TABLE_NAME = ic.TABLE_NAME
                          AND i.INDEX_NAME = ic.INDEX_NAME
                        WHERE i.TABLE_SCHEMA = '' AND i.TABLE_NAME = @table
                          AND i.INDEX_TYPE = 'INDEX' AND i.IS_UNIQUE
                          AND ic.ORDINAL_POSITION IS NOT NULL
                        ORDER BY ic.INDEX_NAME, ic.ORDINAL_POSITION
                        """)
                .bind("table").to(table.table())
                .build())) {
            while (rs.next()) {
                uniqueIndexes.computeIfAbsent(rs.getString(0), k -> new ArrayList<>())
                        .add(rs.getString(1));
            }
        }

        final List<LookupKey> candidates = new ArrayList<>();
        if (!primaryKey.isEmpty()) {
            candidates.add(LookupKey.primaryKey(primaryKey));
        }
        for (final Map.Entry<String, List<String>> index : uniqueIndexes.entrySet()) {
            candidates.add(LookupKey.index(index.getKey(), index.getValue()));
        }
        return new TableMeta(table.table(), null, schemaBuilder.build(), spannerTypes,
                candidates, primaryKey);
    }

    // ------------------------------------------------------------------
    // Query tables: a parameterized GoogleSQL/GQL statement as a keyed table.
    // ------------------------------------------------------------------

    /** Builds a query table's metadata from explicit fields or analyzeQuery(PLAN). */
    private TableMeta deriveQueryTableMeta(QueryTableConfig config) {
        final Schema schema;
        final List<String> spannerTypes = new ArrayList<>();
        if (config.fields != null) {
            schema = config.fields;
            for (final Schema.Field field : schema.getFields()) {
                spannerTypes.add(toSpannerType(field.getFieldType().getType(), config.name,
                        field.getName()));
            }
        } else {
            schema = deriveQuerySchema(config, spannerTypes);
        }
        final String keyType = spannerTypes.get(fieldIndex(schema, config.keyField));
        if (!isSupportedQueryKeyType(keyType)) {
            throw new IllegalStateException("key field '" + config.keyField
                    + "' of spanner query table '" + config.name + "' has unsupported type "
                    + keyType + " (INT64 / STRING / BYTES are supported)");
        }
        final List<String> primaryKey = List.of(config.keyField);
        return new TableMeta(null, config, schema, spannerTypes,
                List.of(LookupKey.primaryKey(primaryKey)), primaryKey);
    }

    /**
     * Derives the result columns by dry-running the statement through
     * {@code analyzeQuery(PLAN)} — no rows are read. Planning needs the bind
     * parameter's type (= the key type, not yet known), so the analyze is
     * retried across INT64 / STRING / BYTES until one plans; the true key type
     * then comes from the returned metadata (the statement always returns the
     * key column).
     */
    private Schema deriveQuerySchema(QueryTableConfig config, List<String> spannerTypes) {
        com.google.cloud.spanner.SpannerException last = null;
        for (final String candidate : List.of("INT64", "STRING", "BYTES")) {
            final Statement statement =
                    dummyBind(Statement.newBuilder(config.sql), config, candidate).build();
            try (ReadContext context = singleUse();
                 ResultSet rs = context.analyzeQuery(statement,
                         ReadContext.QueryAnalyzeMode.PLAN)) {
                // PLAN mode yields no rows; next() drives the RPC so metadata is populated.
                rs.next();
                final com.google.spanner.v1.ResultSetMetadata metadata = rs.getMetadata();
                if (metadata != null && metadata.getRowType().getFieldsCount() > 0) {
                    return buildDerivedSchema(config, metadata.getRowType(), spannerTypes);
                }
            } catch (com.google.cloud.spanner.SpannerException e) {
                last = e;
            }
        }
        throw new IllegalStateException("could not derive the result schema of spanner query"
                + " table '" + config.name + "' via analyzeQuery (tried key types"
                + " [INT64, STRING, BYTES]); specify fields explicitly."
                + (last != null ? " Last error: " + last.getMessage() : ""), last);
    }

    private Schema buildDerivedSchema(QueryTableConfig config,
            com.google.spanner.v1.StructType rowType, List<String> spannerTypes) {
        final Schema.Builder builder = Schema.builder();
        for (final com.google.spanner.v1.StructType.Field field : rowType.getFieldsList()) {
            final String baseType = switch (field.getType().getCode()) {
                case BOOL -> "BOOL";
                case INT64 -> "INT64";
                case FLOAT64 -> "FLOAT64";
                case FLOAT32 -> "FLOAT32";
                case NUMERIC -> "NUMERIC";
                case STRING -> "STRING";
                case BYTES -> "BYTES";
                case DATE -> "DATE";
                case TIMESTAMP -> "TIMESTAMP";
                default -> throw new IllegalStateException("unsupported result column type "
                        + field.getType().getCode() + " for column '" + field.getName()
                        + "' of spanner query table '" + config.name
                        + "'; specify fields explicitly");
            };
            builder.withField(field.getName(), toFieldType(baseType));
            spannerTypes.add(baseType);
        }
        final Schema schema = builder.build();
        if (fieldIndex(schema, config.keyField) < 0) {
            throw new IllegalStateException("query of spanner query table '" + config.name
                    + "' must return a column named '" + config.keyField + "' (the key field)");
        }
        return schema;
    }

    /** Binds the query parameter with an empty/null placeholder of the candidate type. */
    private static Statement.Builder dummyBind(Statement.Builder builder,
            QueryTableConfig config, String candidate) {
        if (config.bindMode == BindMode.ARRAY) {
            switch (candidate) {
                case "INT64" -> builder.bind(config.paramName)
                        .toInt64Array((Iterable<Long>) List.<Long>of());
                case "STRING" -> builder.bind(config.paramName).toStringArray(List.of());
                case "BYTES" -> builder.bind(config.paramName).toBytesArray(List.of());
                default -> throw new IllegalStateException("unexpected candidate: " + candidate);
            }
        } else {
            switch (candidate) {
                case "INT64" -> builder.bind(config.paramName).to((Long) null);
                case "STRING" -> builder.bind(config.paramName).to((String) null);
                case "BYTES" -> builder.bind(config.paramName).to((ByteArray) null);
                default -> throw new IllegalStateException("unexpected candidate: " + candidate);
            }
        }
        return builder;
    }

    /** Runs the query table's statement for the batch (ARRAY: once; PER_KEY: per key). */
    private Iterable<Object[]> queryLookup(TableMeta tableMeta, LookupBatch batch,
            int[] projects) {
        final QueryTableConfig config = tableMeta.query;
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(tableMeta.schema.countFields());
        final String keyType = tableMeta.spannerType(config.keyField);
        final String label = "Spanner query table '" + getName() + "." + config.name + "'";

        if (config.bindMode == BindMode.PER_KEY) {
            // A fresh single-use read per key: a single-use ReadContext is
            // invalidated after one query, so it cannot be shared across keys.
            return PerKeyLookup.run(batch, label, keyValues -> {
                final Statement statement = bindScalar(Statement.newBuilder(config.sql),
                        config.paramName, keyValues.get(0), keyType).build();
                return runQuery(statement, tableMeta, outCols);
            });
        }

        // ARRAY: bind the batch's distinct keys to one array param, run once.
        final Set<Object> distinct = new LinkedHashSet<>();
        for (final LookupRequest request : batch.requests()) {
            if (request.isRange()) {
                throw new IllegalStateException(label
                        + " supports only point equality on its key");
            }
            final Object key = request.prefix().get(0);
            if (key != null) {
                distinct.add(key);
            }
        }
        if (distinct.isEmpty()) {
            return List.of();
        }
        final Statement statement = bindArray(Statement.newBuilder(config.sql),
                config.paramName, distinct, keyType).build();
        return runQuery(statement, tableMeta, outCols);
    }

    /** Executes the statement and decodes rows by column name (not position). */
    private List<Object[]> runQuery(Statement statement, TableMeta tableMeta, int[] outCols) {
        final List<Object[]> rows = new ArrayList<>();
        try (ReadContext context = singleUse();
             ResultSet rs = context.executeQuery(statement)) {
            while (rs.next()) {
                final Object[] row = new Object[outCols.length];
                for (int i = 0; i < outCols.length; i++) {
                    final String field = tableMeta.schema.getField(outCols[i]).getName();
                    final int col;
                    try {
                        col = rs.getColumnIndex(field);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalStateException("query of spanner query table '"
                                + tableMeta.query.name + "' must return a column named '"
                                + field + "' (matching the schema field)", e);
                    }
                    row[i] = decode(rs, col, tableMeta.spannerTypes.get(outCols[i]));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static Statement.Builder bindArray(Statement.Builder builder, String paramName,
            Set<Object> keys, String keyType) {
        switch (keyType) {
            case "INT64" -> {
                final List<Long> values = new ArrayList<>(keys.size());
                for (final Object key : keys) {
                    values.add(((Number) key).longValue());
                }
                builder.bind(paramName).toInt64Array(values);
            }
            case "STRING" -> {
                final List<String> values = new ArrayList<>(keys.size());
                for (final Object key : keys) {
                    values.add(key.toString());
                }
                builder.bind(paramName).toStringArray(values);
            }
            case "BYTES" -> {
                final List<ByteArray> values = new ArrayList<>(keys.size());
                for (final Object key : keys) {
                    values.add(ByteArray.copyFrom(((ByteString) key).getBytes()));
                }
                builder.bind(paramName).toBytesArray(values);
            }
            default -> throw new IllegalStateException(
                    "unsupported spanner query key type: " + keyType);
        }
        return builder;
    }

    private static Statement.Builder bindScalar(Statement.Builder builder, String paramName,
            Object value, String keyType) {
        switch (keyType) {
            case "INT64" -> builder.bind(paramName).to(((Number) value).longValue());
            case "STRING" -> builder.bind(paramName).to(value.toString());
            case "BYTES" -> builder.bind(paramName)
                    .to(ByteArray.copyFrom(((ByteString) value).getBytes()));
            default -> throw new IllegalStateException(
                    "unsupported spanner query key type: " + keyType);
        }
        return builder;
    }

    private static boolean isSupportedQueryKeyType(String spannerType) {
        return switch (spannerType) {
            case "INT64", "STRING", "BYTES" -> true;
            default -> false;
        };
    }

    /** Explicit field type → the Spanner base type the decoder reads it as. */
    private static String toSpannerType(Schema.Type type, String tableName, String fieldName) {
        return switch (type) {
            case bool -> "BOOL";
            case int8, int16, int32, int64 -> "INT64";
            case float32 -> "FLOAT32";
            case float64 -> "FLOAT64";
            case decimal -> "NUMERIC";
            case string, enumeration -> "STRING";
            case json -> "JSON";
            case bytes -> "BYTES";
            case date -> "DATE";
            case time -> "STRING";
            case timestamp -> "TIMESTAMP";
            default -> throw new IllegalArgumentException("unsupported field type " + type
                    + " for field '" + fieldName + "' of spanner query table '"
                    + tableName + "'");
        };
    }

    /** Strips length/parameters: {@code STRING(MAX)} → {@code STRING}. */
    private static String baseType(String spannerType) {
        int end = spannerType.indexOf('(');
        if (end < 0) {
            end = spannerType.indexOf('<');
        }
        return (end < 0 ? spannerType : spannerType.substring(0, end)).trim();
    }

    private static Schema.FieldType toFieldType(String baseType) {
        return switch (baseType) {
            case "BOOL" -> Schema.FieldType.BOOLEAN;
            case "INT64", "ENUM" -> Schema.FieldType.INT64;
            case "FLOAT64", "FLOAT32" -> Schema.FieldType.FLOAT64;
            case "NUMERIC" -> Schema.FieldType.DECIMAL;
            case "STRING" -> Schema.FieldType.STRING;
            case "JSON" -> Schema.FieldType.JSON;
            case "BYTES" -> Schema.FieldType.BYTES;
            case "DATE" -> Schema.FieldType.DATE;
            case "TIMESTAMP" -> Schema.FieldType.TIMESTAMP;
            default -> null; // ARRAY / STRUCT / TOKENLIST / PROTO: skipped
        };
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
                    "spanner lookup source '" + getName() + "' is not set up");
        }
        final TableMeta tableMeta = meta.get(table);
        if (tableMeta == null) {
            throw new IllegalStateException("unknown lookup table: " + getName() + "." + table);
        }
        return tableMeta;
    }
}
