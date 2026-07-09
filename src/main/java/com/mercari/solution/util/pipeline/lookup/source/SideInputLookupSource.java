package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link LookupSource} over another {@code PCollection}'s elements delivered as a
 * Beam <em>side input</em>: joins the per-element SQL of the {@code query} transform
 * against in-pipeline data with no external store.
 *
 * <p>Unlike the other adapters there are no clients and no launcher-side metadata
 * derivation: the table schema and key columns come from the configuration (the
 * side input collection's schema), so planning needs no connectivity at all. The
 * data arrives at runtime: the hosting {@code DoFn} calls
 * {@link #setData(String, Iterable, Object)} with the side input's current contents
 * before each evaluation. Rows are converted to Calcite-internal values and indexed
 * by the declared key <em>once per window token</em> (not per element): repeated
 * {@code setData} calls with an equal token are no-ops, and lookups are served from
 * a hash index per constrained prefix length, built lazily on first use.
 *
 * <p>Point, key-prefix and prefix+bounded-range lookups are supported (the whole
 * data set is in memory, so a prefix group can be filtered linearly for ranges).
 * The side input is broadcast to every worker and held indexed on heap — intended
 * for dimension-sized collections; use the jdbc/spanner/bigtable adapters for data
 * that does not fit in memory.
 */
public class SideInputLookupSource extends LookupSource {

    /** One exposed table bound to one side input collection. */
    public static class TableConfig implements Serializable {

        private final String name;           // SQL table name
        private final String input;          // side input (module output) name
        private final List<String> keyFields;
        private final Schema schema;
        private final int[] keyIndexes;      // key column ordinals in schema order

        private TableConfig(String name, String input, List<String> keyFields,
                Schema schema, int[] keyIndexes) {
            this.name = name;
            this.input = input;
            this.keyFields = keyFields;
            this.schema = schema;
            this.keyIndexes = keyIndexes;
        }

        public String getInput() {
            return input;
        }
    }

    public static class TableBuilder {

        private String name;
        private String input;
        private List<String> keyFields;
        private Schema schema;

        public TableBuilder withName(String name) {
            this.name = name;
            return this;
        }

        /** The side input collection name; defaults to the table name (and vice versa). */
        public TableBuilder withInput(String input) {
            this.input = input;
            return this;
        }

        /** Key columns in key order (the primary key the lookup-join constrains). */
        public TableBuilder withKeyFields(List<String> keyFields) {
            this.keyFields = new ArrayList<>(keyFields);
            return this;
        }

        /** The side input collection's schema. */
        public TableBuilder withSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public TableConfig build() {
            final String tableName = name != null ? name : input;
            final String inputName = input != null ? input : name;
            if (tableName == null) {
                throw new IllegalArgumentException(
                        "sideinput table requires name or input");
            }
            if (schema == null) {
                throw new IllegalArgumentException(
                        "sideinput table '" + tableName + "' requires a schema");
            }
            if (keyFields == null || keyFields.isEmpty()) {
                throw new IllegalArgumentException(
                        "sideinput table '" + tableName + "' requires keyFields");
            }
            final int[] keyIndexes = new int[keyFields.size()];
            for (int i = 0; i < keyFields.size(); i++) {
                keyIndexes[i] = fieldOrdinal(schema, keyFields.get(i));
                if (keyIndexes[i] < 0) {
                    throw new IllegalArgumentException(
                            "sideinput table '" + tableName + "' keyField '" + keyFields.get(i)
                                    + "' is not a field of the side input schema: "
                                    + schema.getFields().stream().map(Schema.Field::getName).toList());
                }
            }
            return new TableConfig(tableName, inputName, List.copyOf(keyFields), schema, keyIndexes);
        }
    }

    public static class Builder {

        private String name;
        private final List<TableConfig> tables = new ArrayList<>();

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withTable(TableConfig table) {
            this.tables.add(table);
            return this;
        }

        public SideInputLookupSource build() {
            if (tables.isEmpty()) {
                throw new IllegalArgumentException(
                        "sideinput source '" + name + "' requires at least one table");
            }
            return new SideInputLookupSource(name, tables);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TableBuilder table() {
        return new TableBuilder();
    }

    private final List<TableConfig> tables;

    // Per-table runtime state: the side input contents of the current window,
    // converted and indexed lazily. Rebuilt only when the window token changes.
    private transient Map<String, TableData> data;

    private static final class TableData {
        Object token;
        Iterable<MElement> pending;                          // unconverted side input
        List<Object[]> rows;                                 // Calcite-internal full rows
        final Map<Integer, Map<List<Object>, List<Object[]>>> indexes = new HashMap<>();
    }

    private SideInputLookupSource(String name, List<TableConfig> tables) {
        super(name);
        final LinkedHashSet<String> names = new LinkedHashSet<>();
        for (final TableConfig table : tables) {
            if (!names.add(table.name)) {
                throw new IllegalArgumentException(
                        "sideinput source '" + name + "' has duplicate table name: " + table.name);
            }
        }
        this.tables = List.copyOf(tables);
    }

    /** The distinct side input collection names this source's tables read. */
    public List<String> inputNames() {
        final LinkedHashSet<String> names = new LinkedHashSet<>();
        for (final TableConfig table : tables) {
            names.add(table.input);
        }
        return new ArrayList<>(names);
    }

    /**
     * Hands this source the current contents of side input {@code input}. Call
     * before every evaluation; when {@code token} equals the previous call's
     * non-null token (same window), the call is a no-op and the existing index
     * is kept — a {@code null} token always replaces the data. The elements are
     * not read here — conversion and indexing happen lazily on the first lookup
     * that needs them.
     */
    public void setData(String input, Iterable<MElement> elements, Object token) {
        if (data == null) {
            throw new IllegalStateException(
                    "sideinput source '" + getName() + "' is not set up");
        }
        for (final TableConfig table : tables) {
            if (!table.input.equals(input)) {
                continue;
            }
            final TableData tableData = data.get(table.name);
            final boolean sameToken = token != null && Objects.equals(tableData.token, token);
            if (sameToken && (tableData.pending != null || tableData.rows != null)) {
                continue;
            }
            tableData.token = token;
            tableData.pending = elements;
            tableData.rows = null;
            tableData.indexes.clear();
        }
    }

    @Override
    protected void setupInternal() {
        if (data == null) {
            data = new HashMap<>();
            for (final TableConfig table : tables) {
                data.put(table.name, new TableData());
            }
        }
    }

    @Override
    protected void closeInternal() {
        data = null;
    }

    @Override
    public Map<String, Schema> tableSchemas() {
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        for (final TableConfig table : tables) {
            schemas.put(table.name, table.schema);
        }
        return schemas;
    }

    @Override
    public List<LookupKey> keyCandidates(String table) {
        for (final TableConfig config : tables) {
            if (config.name.equals(table)) {
                return List.of(LookupKey.primaryKey(config.keyFields));
            }
        }
        return List.of();
    }

    @Override
    public boolean supportsKeyPrefixLookup() {
        return true;
    }

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        final TableConfig config = tableConfig(table);
        final TableData tableData = data == null ? null : data.get(table);
        if (tableData == null || (tableData.pending == null && tableData.rows == null)) {
            throw new IllegalStateException(
                    "side input data for table '" + getName() + "." + table
                            + "' has not been provided (setData was not called before execution)");
        }
        final List<Object[]> rows = rows(config, tableData);
        final List<Object[]> result = new ArrayList<>();
        final int prefixLength = batch.prefixLength();
        if (batch.isRange()) {
            final int rangeIndex = config.keyIndexes[prefixLength];
            for (final LookupRequest request : new LinkedHashSet<>(batch.requests())) {
                final List<Object[]> group = prefixGroup(config, tableData, rows,
                        prefixLength, request.prefix());
                for (final Object[] row : group) {
                    if (inRange(row[rangeIndex], request)) {
                        result.add(project(row, projects));
                    }
                }
            }
        } else if (prefixLength == 0) {
            // Not produced by the join operator (a point/prefix batch always
            // constrains at least one column); answer defensively as a superset.
            for (final Object[] row : rows) {
                result.add(project(row, projects));
            }
        } else {
            for (final List<Object> prefix : batch.distinctPrefixes()) {
                for (final Object[] row : prefixGroup(config, tableData, rows,
                        prefixLength, prefix)) {
                    result.add(project(row, projects));
                }
            }
        }
        return result;
    }

    private TableConfig tableConfig(String table) {
        for (final TableConfig config : tables) {
            if (config.name.equals(table)) {
                return config;
            }
        }
        throw new IllegalArgumentException(
                "unknown sideinput table: " + getName() + "." + table);
    }

    /** Converts the pending side input elements once per window token. */
    private List<Object[]> rows(TableConfig config, TableData tableData) {
        if (tableData.rows == null) {
            final List<Object[]> rows = new ArrayList<>();
            for (final MElement element : tableData.pending) {
                if (element != null) {
                    rows.add(CalciteValues.toInternalRow(config.schema, element));
                }
            }
            tableData.rows = rows;
            tableData.pending = null;
        }
        return tableData.rows;
    }

    /**
     * Rows whose leading {@code prefixLength} key columns equal {@code prefix},
     * from a hash index built lazily per prefix length. Rows with a null in an
     * indexed key column are excluded (SQL equality never matches null), as are
     * requests carrying a null component.
     */
    private List<Object[]> prefixGroup(TableConfig config, TableData tableData,
            List<Object[]> rows, int prefixLength, List<Object> prefix) {
        if (prefixLength == 0) {
            return rows;
        }
        // Immutable lists reject contains(null); iterate instead. (A request
        // prefix normally never carries null — equality on null never matches.)
        for (final Object component : prefix) {
            if (component == null) {
                return List.of();
            }
        }
        final Map<List<Object>, List<Object[]>> index = tableData.indexes
                .computeIfAbsent(prefixLength, length -> buildIndex(config, rows, length));
        return index.getOrDefault(prefix, List.of());
    }

    private static Map<List<Object>, List<Object[]>> buildIndex(TableConfig config,
            List<Object[]> rows, int prefixLength) {
        final Map<List<Object>, List<Object[]>> index = new HashMap<>();
        outer:
        for (final Object[] row : rows) {
            final List<Object> key = new ArrayList<>(prefixLength);
            for (int i = 0; i < prefixLength; i++) {
                final Object value = row[config.keyIndexes[i]];
                if (value == null) {
                    continue outer;
                }
                key.add(value);
            }
            index.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return index;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean inRange(Object value, LookupRequest request) {
        if (value == null) {
            return false;
        }
        final Comparable comparable = (Comparable) value;
        if (request.lower() != null) {
            final int c = comparable.compareTo(request.lower());
            if (c < 0 || (c == 0 && !request.lowerInclusive())) {
                return false;
            }
        }
        if (request.upper() != null) {
            final int c = comparable.compareTo(request.upper());
            if (c > 0 || (c == 0 && !request.upperInclusive())) {
                return false;
            }
        }
        return true;
    }

    // Indexed rows are shared across lookups and must not be mutated by callers
    // (the same convention as the base class's lookup cache).
    private static Object[] project(Object[] row, int[] projects) {
        if (projects == null) {
            return row;
        }
        final Object[] projected = new Object[projects.length];
        for (int i = 0; i < projects.length; i++) {
            projected[i] = row[projects[i]];
        }
        return projected;
    }

    private static int fieldOrdinal(Schema schema, String fieldName) {
        for (int i = 0; i < schema.countFields(); i++) {
            if (schema.getField(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }
}
