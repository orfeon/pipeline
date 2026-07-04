package com.mercari.solution.util.pipeline.lookup.source;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.UnsafeByteOperations;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Cloud Bigtable-backed {@link LookupSource}. Each table is defined entirely by
 * configuration: an opaque row-key column plus {@code family:qualifier}-mapped
 * value columns, decoded with the HBase-compatible byte codec (the layout
 * GoogleSQL/BigQuery use to read Bigtable cells; DATE/TIMESTAMP are stored as
 * epoch-microsecond INT64). The only key is the row key — composite-key structure
 * (e.g. {@code user#ts}) is built by the caller in SQL, never modeled here.
 *
 * <p>A lookup turns point requests into row-key point reads and range requests
 * into row-key ranges, all in one {@code readRows} call restricted to the mapped
 * cells (latest version only). Full scans are impossible by construction.
 */
public class BigtableLookupSource extends LookupSource {

    /** One value column: schema field name → {@code family:qualifier} + storage type. */
    private record ColumnConfig(String field, String family, String qualifier,
            Schema.Type storageType) implements Serializable {
    }

    /** One exposed table. */
    private static class TableConfig implements Serializable {

        private final String name;
        private final String tableId;
        private final String rowKeyField;
        private final Schema.Type rowKeyType;
        private final List<ColumnConfig> columns;
        private final Schema schema;   // rowKeyField first, then value columns

        private TableConfig(String name, String tableId, String rowKeyField,
                Schema.Type rowKeyType, List<ColumnConfig> columns) {
            this.name = name;
            this.tableId = tableId;
            this.rowKeyField = rowKeyField;
            this.rowKeyType = rowKeyType;
            this.columns = columns;
            final Schema.Builder builder = Schema.builder();
            builder.withField(rowKeyField, surfacedType(rowKeyType));
            for (final ColumnConfig column : columns) {
                builder.withField(column.field(), surfacedType(column.storageType()));
            }
            this.schema = builder.build();
        }

        private Schema.Type storageType(int fieldIndex) {
            return fieldIndex == 0 ? rowKeyType : columns.get(fieldIndex - 1).storageType();
        }
    }

    public static class TableBuilder {

        private String name;
        private String tableId;
        private String rowKeyField = "rowKey";
        private Schema.Type rowKeyType = Schema.Type.string;
        private final List<ColumnConfig> columns = new ArrayList<>();

        public TableBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public TableBuilder withTableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public TableBuilder withRowKeyField(String rowKeyField) {
            this.rowKeyField = rowKeyField;
            return this;
        }

        public TableBuilder withRowKeyType(Schema.Type rowKeyType) {
            this.rowKeyType = rowKeyType;
            return this;
        }

        public TableBuilder withColumn(String field, String family, String qualifier,
                Schema.Type type) {
            this.columns.add(new ColumnConfig(field, family,
                    qualifier == null ? field : qualifier, type));
            return this;
        }

        public TableConfig build() {
            if (name == null) {
                throw new IllegalArgumentException("bigtable table requires name");
            }
            checkCodecSupport(rowKeyType);
            for (final ColumnConfig column : columns) {
                checkCodecSupport(column.storageType());
            }
            return new TableConfig(name, tableId == null ? name : tableId,
                    rowKeyField, rowKeyType, List.copyOf(columns));
        }
    }

    public static TableBuilder table() {
        return new TableBuilder();
    }

    private final String projectId;
    private final String instanceId;
    private final String appProfileId;
    private final Map<String, TableConfig> tables;

    private transient BigtableDataClient client;

    private BigtableLookupSource(Builder builder) {
        super(builder.name);
        this.projectId = Objects.requireNonNull(builder.projectId, "projectId must not be null");
        this.instanceId = Objects.requireNonNull(builder.instanceId, "instanceId must not be null");
        this.appProfileId = builder.appProfileId;
        final Map<String, TableConfig> map = new LinkedHashMap<>();
        for (final TableConfig table : builder.tables) {
            map.put(table.name, table);
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException(
                    "bigtable lookup source requires at least one table");
        }
        this.tables = map;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private String projectId;
        private String instanceId;
        private String appProfileId;
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

        public Builder withAppProfileId(String appProfileId) {
            this.appProfileId = appProfileId;
            return this;
        }

        public Builder withTable(TableConfig table) {
            this.tables.add(table);
            return this;
        }

        public BigtableLookupSource build() {
            return new BigtableLookupSource(this);
        }
    }

    @Override
    protected void setupInternal() {
        if (client == null) {
            try {
                final BigtableDataSettings.Builder settings = BigtableDataSettings.newBuilder()
                        .setProjectId(projectId)
                        .setInstanceId(instanceId);
                if (appProfileId != null) {
                    settings.setAppProfileId(appProfileId);
                }
                this.client = BigtableDataClient.create(settings.build());
            } catch (IOException e) {
                throw new IllegalStateException(
                        "failed to create bigtable client for lookup source: " + getName(), e);
            }
        }
    }

    @Override
    protected void closeInternal() {
        if (client != null) {
            try {
                client.close();
            } finally {
                client = null;
            }
        }
    }

    @Override
    public Map<String, Schema> tableSchemas() {
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        for (final TableConfig table : tables.values()) {
            schemas.put(table.name, table.schema);
        }
        return schemas;
    }

    @Override
    public List<LookupKey> keyCandidates(String table) {
        final TableConfig config = tables.get(table);
        return config == null
                ? List.of() : List.of(LookupKey.primaryKey(List.of(config.rowKeyField)));
    }

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        // Bigtable has only the row key; indexName is always null.
        final TableConfig config = tables.get(table);
        if (config == null) {
            throw new IllegalStateException("unknown lookup table: " + getName() + "." + table);
        }
        final Map<String, RowKeyRange> distinct = new LinkedHashMap<>();
        for (final LookupRequest request : batch.requests()) {
            final RowKeyRange range;
            if (request.isRange()) {
                range = new RowKeyRange(
                        encode(request.lower(), config.rowKeyType), request.lowerInclusive(),
                        encode(request.upper(), config.rowKeyType), request.upperInclusive());
            } else {
                final byte[] key = encode(request.prefix().get(0), config.rowKeyType);
                range = new RowKeyRange(key, true, key, true);
            }
            distinct.putIfAbsent(range.cacheKey(), range);
        }
        if (distinct.isEmpty()) {
            return List.of();
        }

        final Query query = Query.create(config.tableId);
        for (final RowKeyRange range : distinct.values()) {
            if (range.isPoint()) {
                query.rowKey(UnsafeByteOperations.unsafeWrap(range.lower));
            } else {
                ByteStringRange btRange = ByteStringRange.unbounded();
                final com.google.protobuf.ByteString lower =
                        UnsafeByteOperations.unsafeWrap(range.lower);
                btRange = range.lowerInclusive
                        ? btRange.startClosed(lower) : btRange.startOpen(lower);
                final com.google.protobuf.ByteString upper =
                        UnsafeByteOperations.unsafeWrap(range.upper);
                btRange = range.upperInclusive
                        ? btRange.endClosed(upper) : btRange.endOpen(upper);
                query.range(btRange);
            }
        }
        final Filters.Filter filter = schemaCellFilter(config);
        if (filter != null) {
            query.filter(filter);
        }

        final List<Object[]> rows = new ArrayList<>();
        for (final Row row : client.readRows(query)) {
            rows.add(decodeRow(row, config, projects));
        }
        return rows;
    }

    /**
     * A filter selecting only the schema's mapped cells (latest version each),
     * or {@code null} when the table has no value columns (row key only).
     */
    private static Filters.Filter schemaCellFilter(TableConfig config) {
        if (config.columns.isEmpty()) {
            return null;
        }
        final Filters.InterleaveFilter columnFilter = Filters.FILTERS.interleave();
        for (final ColumnConfig column : config.columns) {
            columnFilter.filter(Filters.FILTERS.chain()
                    .filter(Filters.FILTERS.family().exactMatch(column.family()))
                    .filter(Filters.FILTERS.qualifier().exactMatch(column.qualifier())));
        }
        return Filters.FILTERS.chain()
                .filter(columnFilter)
                .filter(Filters.FILTERS.limit().cellsPerColumn(1));
    }

    /** Decodes one Bigtable row into a Calcite row, honoring the projection. */
    private static Object[] decodeRow(Row row, TableConfig config, int[] projects) {
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(config.schema.countFields());
        final Object[] out = new Object[outCols.length];
        for (int i = 0; i < outCols.length; i++) {
            final int column = outCols[i];
            final Schema.Type storageType = config.storageType(column);
            if (column == 0) {
                out[i] = decode(row.getKey().toByteArray(), storageType);
            } else {
                final ColumnConfig mapping = config.columns.get(column - 1);
                final List<RowCell> cells = row.getCells(mapping.family(), mapping.qualifier());
                if (cells.isEmpty()) {
                    out[i] = null;
                } else {
                    // Cells are returned latest-first; take the most recent version.
                    out[i] = decode(cells.get(0).getValue().toByteArray(), storageType);
                }
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // HBase-compatible byte codec (matching org.apache.hadoop.hbase.util.Bytes,
    // without an HBase dependency; the layout GoogleSQL/BigQuery use).
    // DATE/TIMESTAMP are stored as epoch-microseconds, 8-byte big-endian.
    // ------------------------------------------------------------------

    private static final long MICROS_PER_MILLI = 1_000L;
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    /** float32 cells are stored as 4-byte floats but surfaced as FLOAT64 in SQL. */
    private static Schema.FieldType surfacedType(Schema.Type storageType) {
        return switch (storageType) {
            case float32 -> Schema.FieldType.FLOAT64;
            default -> Schema.FieldType.type(storageType);
        };
    }

    private static void checkCodecSupport(Schema.Type type) {
        switch (type) {
            case string, bool, int32, int64, float32, float64, bytes, date, timestamp -> {
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported Bigtable codec type: " + type);
        }
    }

    /** Encodes a Calcite-internal value to its HBase-compatible byte form. */
    private static byte[] encode(Object value, Schema.Type type) {
        return switch (type) {
            case string -> value.toString().getBytes(StandardCharsets.UTF_8);
            case bool -> new byte[]{(byte) (((Boolean) value) ? 1 : 0)};
            case int32 -> toBytes(((Number) value).intValue());
            case int64 -> toBytes(((Number) value).longValue());
            case float32 -> toBytes(Float.floatToIntBits(((Number) value).floatValue()));
            case float64 -> toBytes(Double.doubleToLongBits(((Number) value).doubleValue()));
            case bytes -> ((ByteString) value).getBytes();
            // Calcite DATE = epoch days (int) → epoch micros.
            case date -> toBytes(((Number) value).longValue() * MICROS_PER_DAY);
            // Calcite TIMESTAMP = epoch millis (long) → epoch micros.
            case timestamp -> toBytes(((Number) value).longValue() * MICROS_PER_MILLI);
            default -> throw new IllegalStateException("Unsupported Bigtable codec type: " + type);
        };
    }

    /** Decodes HBase-compatible bytes into a Calcite-internal value. */
    private static Object decode(byte[] bytes, Schema.Type type) {
        return switch (type) {
            case string -> new String(bytes, StandardCharsets.UTF_8);
            case bool -> {
                requireLength(bytes, 1, type);
                yield bytes[0] != 0;
            }
            case int32 -> {
                requireLength(bytes, 4, type);
                yield toInt(bytes);
            }
            case int64 -> {
                requireLength(bytes, 8, type);
                yield toLong(bytes);
            }
            case float32 -> {
                requireLength(bytes, 4, type);
                yield (double) Float.intBitsToFloat(toInt(bytes));
            }
            case float64 -> {
                requireLength(bytes, 8, type);
                yield Double.longBitsToDouble(toLong(bytes));
            }
            case bytes -> new ByteString(bytes.clone());
            case date -> {
                requireLength(bytes, 8, type);
                yield (int) (toLong(bytes) / MICROS_PER_DAY);
            }
            case timestamp -> {
                requireLength(bytes, 8, type);
                yield toLong(bytes) / MICROS_PER_MILLI;
            }
            default -> throw new IllegalStateException("Unsupported Bigtable codec type: " + type);
        };
    }

    private static byte[] toBytes(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    private static byte[] toBytes(long v) {
        return new byte[]{
                (byte) (v >> 56), (byte) (v >> 48), (byte) (v >> 40), (byte) (v >> 32),
                (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v
        };
    }

    private static int toInt(byte[] b) {
        return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16)
                | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
    }

    private static long toLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (b[i] & 0xFFL);
        }
        return result;
    }

    private static void requireLength(byte[] bytes, int expected, Schema.Type type) {
        if (bytes.length != expected) {
            throw new IllegalStateException("Invalid byte length " + bytes.length
                    + " for " + type + " (expected " + expected + ")");
        }
    }

    /** A single row-key constraint: a point or a bounded range, in encoded bytes. */
    private static final class RowKeyRange {

        private final byte[] lower;
        private final boolean lowerInclusive;
        private final byte[] upper;
        private final boolean upperInclusive;

        private RowKeyRange(byte[] lower, boolean lowerInclusive,
                byte[] upper, boolean upperInclusive) {
            this.lower = lower;
            this.lowerInclusive = lowerInclusive;
            this.upper = upper;
            this.upperInclusive = upperInclusive;
        }

        private boolean isPoint() {
            return lowerInclusive && upperInclusive && Arrays.equals(lower, upper);
        }

        private String cacheKey() {
            return (lowerInclusive ? '[' : '(') + hex(lower)
                    + ".." + hex(upper) + (upperInclusive ? ']' : ')');
        }

        private static String hex(byte[] bytes) {
            final StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (final byte b : bytes) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16))
                        .append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        }
    }
}
