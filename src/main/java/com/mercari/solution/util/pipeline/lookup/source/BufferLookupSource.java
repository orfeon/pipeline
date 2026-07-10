package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link LookupSource} over the {@code query} transform's <em>own past input
 * elements</em>, accumulated per group key in Beam state ({@code OrderedListState})
 * by the hosting stateful {@code DoFn}: the per-element SQL joins (typically via
 * {@code JOIN LATERAL}) against the history of the current element's group —
 * sequence detection, funnels, per-key aggregates over recent events — with no
 * external store.
 *
 * <p>Like {@link SideInputLookupSource} there are no clients and no launcher-side
 * derivation: the table schema is the input schema plus two synthetic columns —
 * {@value #TIMESTAMP_FIELD} (the buffered element's event time) and
 * {@value #INPUT_FIELD} (the name of the input the element came from). The
 * candidate key is {@code groupFields + __timestamp}, so the key contract allows
 * equality on the group fields alone (prefix), or plus a bounded range /
 * equality on {@code __timestamp}. The hosting {@code DoFn} calls
 * {@link #setData(List, MElement)} with the visible buffer contents before every
 * evaluation (unlike the side input source there is no window token — the buffer
 * changes with every element).
 *
 * <p><b>Key affinity</b>: Beam state is partitioned by the {@code DoFn}'s key, so
 * this source can only answer lookups for the <em>current element's own group</em>.
 * {@link #validateLookupBinding} rejects at planning time any join that does not
 * constrain every group field to the input column of the same name;
 * {@link #lookup} additionally verifies at runtime that each request's key prefix
 * equals the current element's group values, as a backstop against bindings the
 * name-based validation could not see through.
 *
 * <p>The rules' {@link #markLookupUsage} calls are accumulated into
 * {@link #referencedColumns()} during construction-time planning, letting the
 * transform narrow which input fields are persisted in state.
 */
public class BufferLookupSource extends LookupSource {

    /** Synthetic column: the buffered element's event time (TIMESTAMP). */
    public static final String TIMESTAMP_FIELD = "__timestamp";
    /** Synthetic column: the name of the input the buffered element came from (STRING). */
    public static final String INPUT_FIELD = "__input";

    public static class Builder {

        private String name;
        private String table;
        private List<String> groupFields;
        private Schema inputSchema;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        /** The SQL table name the buffer is referenced by ({@code sourceName.table}). */
        public Builder withTable(String table) {
            this.table = table;
            return this;
        }

        /** The state key fields, in key order (the columns a lookup must constrain). */
        public Builder withGroupFields(List<String> groupFields) {
            this.groupFields = new ArrayList<>(groupFields);
            return this;
        }

        /** The (union) schema of the transform's input elements. */
        public Builder withInputSchema(Schema inputSchema) {
            this.inputSchema = inputSchema;
            return this;
        }

        public BufferLookupSource build() {
            if (table == null) {
                throw new IllegalArgumentException("buffer source requires a table name");
            }
            if (groupFields == null || groupFields.isEmpty()) {
                throw new IllegalArgumentException(
                        "buffer source '" + name + "' requires groupFields");
            }
            if (inputSchema == null) {
                throw new IllegalArgumentException(
                        "buffer source '" + name + "' requires the input schema");
            }
            final List<String> inputFieldNames = inputSchema.getFields().stream()
                    .map(Schema.Field::getName).toList();
            for (final String groupField : groupFields) {
                if (!inputFieldNames.contains(groupField)) {
                    throw new IllegalArgumentException(
                            "buffer source '" + name + "' groupField '" + groupField
                                    + "' is not a field of the input schema: " + inputFieldNames);
                }
            }
            for (final String reserved : List.of(TIMESTAMP_FIELD, INPUT_FIELD)) {
                if (inputFieldNames.contains(reserved)) {
                    throw new IllegalArgumentException(
                            "buffer source '" + name + "' cannot be used with an input schema"
                                    + " that already has a field named '" + reserved
                                    + "' (reserved for the synthetic buffer column);"
                                    + " rename the input field first");
                }
            }
            final List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
            fields.add(Schema.Field.of(TIMESTAMP_FIELD, Schema.FieldType.TIMESTAMP));
            fields.add(Schema.Field.of(INPUT_FIELD, Schema.FieldType.STRING.withNullable(true)));
            return new BufferLookupSource(name, table, groupFields, Schema.of(fields));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final String table;
    private final List<String> groupFields;
    private final Schema tableSchema;
    private final int[] keyIndexes;             // groupFields + __timestamp ordinals
    // Buffer-table columns the planned SQL uses, accumulated by the planner
    // rules during construction-time planning (serialized to workers with the
    // instance so the host's stored-fields decision and the worker agree).
    private final LinkedHashSet<String> referencedColumns = new LinkedHashSet<>();

    // Per-element runtime state: the visible buffer contents, converted and
    // indexed lazily; replaced by setData before every evaluation.
    private transient List<MElement> pending;
    private transient List<Object[]> rows;
    private transient Map<Integer, Map<List<Object>, List<Object[]>>> indexes;
    private transient List<Object> currentKey;  // current element's group values (internal)
    private transient boolean hasData;

    private BufferLookupSource(String name, String table, List<String> groupFields,
            Schema tableSchema) {
        super(name);
        this.table = table;
        this.groupFields = List.copyOf(groupFields);
        this.tableSchema = tableSchema;
        this.keyIndexes = new int[groupFields.size() + 1];
        for (int i = 0; i < groupFields.size(); i++) {
            this.keyIndexes[i] = fieldOrdinal(tableSchema, groupFields.get(i));
        }
        this.keyIndexes[groupFields.size()] = fieldOrdinal(tableSchema, TIMESTAMP_FIELD);
    }

    public String tableName() {
        return table;
    }

    public List<String> groupFields() {
        return groupFields;
    }

    /** The buffer-table columns the planned SQL references (after planning). */
    public Set<String> referencedColumns() {
        return referencedColumns;
    }

    /**
     * Builds the element persisted in state (and fed back via {@link #setData}):
     * the element's {@code storedFields} primitive values plus the synthetic
     * {@value #TIMESTAMP_FIELD} / {@value #INPUT_FIELD} columns.
     */
    public static MElement createBufferElement(
            final MElement element,
            final org.joda.time.Instant timestamp,
            final String inputName,
            final Collection<String> storedFields) {

        final Map<String, Object> values = new HashMap<>();
        for (final String field : storedFields) {
            values.put(field, element.getPrimitiveValue(field));
        }
        values.put(TIMESTAMP_FIELD, timestamp.getMillis() * 1000L); // epoch micros
        values.put(INPUT_FIELD, inputName);
        return MElement.of(values, timestamp);
    }

    /**
     * Hands this source the buffer contents visible to the next evaluation
     * (buffer elements created by {@link #createBufferElement}, oldest first,
     * already including the current element when {@code includeCurrent}), and
     * the current element in buffer form — whose group values every lookup
     * request must match. Call before every evaluation.
     */
    public void setData(List<MElement> visible, MElement current) {
        if (indexes == null) {
            throw new IllegalStateException(
                    "buffer source '" + getName() + "' is not set up");
        }
        this.pending = visible;
        this.rows = null;
        this.indexes.clear();
        this.hasData = true;
        final List<Object> key = new ArrayList<>(groupFields.size());
        for (int i = 0; i < groupFields.size(); i++) {
            final Schema.Field field = tableSchema.getField(keyIndexes[i]);
            key.add(CalciteValues.toInternal(field.getFieldType(),
                    current.getPrimitiveValue(field.getName())));
        }
        this.currentKey = key;
    }

    @Override
    protected void setupInternal() {
        if (indexes == null) {
            indexes = new HashMap<>();
        }
    }

    @Override
    protected void closeInternal() {
        pending = null;
        rows = null;
        indexes = null;
        currentKey = null;
        hasData = false;
    }

    @Override
    public Map<String, Schema> tableSchemas() {
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        schemas.put(table, tableSchema);
        return schemas;
    }

    @Override
    public List<LookupKey> keyCandidates(String table) {
        if (!this.table.equals(table)) {
            return List.of();
        }
        final List<String> columns = new ArrayList<>(groupFields);
        columns.add(TIMESTAMP_FIELD);
        return List.of(LookupKey.primaryKey(columns));
    }

    @Override
    public boolean supportsKeyPrefixLookup() {
        return true;
    }

    @Override
    public void validateLookupBinding(
            String table, String indexName, int prefixLength, List<String> boundFieldNames) {
        // The buffer only holds the current element's own group: every group
        // field must be constrained by equality to the input column of the
        // same name — a shorter prefix or a differently-bound key would
        // silently miss data of other groups.
        if (prefixLength < groupFields.size()) {
            throw new IllegalArgumentException(
                    "buffer table '" + getName() + "." + table + "' requires equality on all"
                            + " groupFields " + groupFields + " (the buffer only holds the"
                            + " current element's own group), but only the first " + prefixLength
                            + " key column(s) are constrained by equality");
        }
        for (int i = 0; i < groupFields.size(); i++) {
            final String bound = boundFieldNames.get(i);
            if (bound == null || !bound.equalsIgnoreCase(groupFields.get(i))) {
                throw new IllegalArgumentException(
                        "buffer table '" + getName() + "." + table + "' key column '"
                                + groupFields.get(i) + "' must be joined to the input column of"
                                + " the same name (the buffer only holds the current element's"
                                + " own group), but was bound to: "
                                + (bound == null ? "a computed expression" : "'" + bound + "'"));
            }
        }
    }

    @Override
    public void markLookupUsage(String table, Collection<String> columns) {
        if (this.table.equals(table)) {
            referencedColumns.addAll(columns);
        }
    }

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        if (!this.table.equals(table)) {
            throw new IllegalArgumentException(
                    "unknown buffer table: " + getName() + "." + table);
        }
        if (!hasData) {
            throw new IllegalStateException(
                    "buffer data for table '" + getName() + "." + table
                            + "' has not been provided (setData was not called before execution)");
        }
        final int prefixLength = batch.prefixLength();
        // Runtime backstop for the key affinity the plan-time validation could
        // not see through: state only holds the current element's own group.
        if (prefixLength < groupFields.size()) {
            throw new IllegalStateException(
                    "buffer table '" + getName() + "." + table + "' lookup constrains only "
                            + prefixLength + " key column(s) but requires equality on all"
                            + " groupFields: " + groupFields);
        }
        for (final LookupRequest request : batch.requests()) {
            for (int i = 0; i < groupFields.size(); i++) {
                if (!Objects.equals(request.prefix().get(i), currentKey.get(i))) {
                    throw new IllegalStateException(
                            "buffer table '" + getName() + "." + table + "' can only be looked"
                                    + " up by the current element's own group key (groupField '"
                                    + groupFields.get(i) + "' = " + currentKey.get(i)
                                    + ") but the query requested: " + request.prefix().get(i)
                                    + " — join the buffer on the input columns " + groupFields);
                }
            }
        }
        final List<Object[]> rows = rows();
        final List<Object[]> result = new ArrayList<>();
        if (batch.isRange()) {
            final int rangeIndex = keyIndexes[prefixLength];
            for (final LookupRequest request : new LinkedHashSet<>(batch.requests())) {
                for (final Object[] row : prefixGroup(rows, prefixLength, request.prefix())) {
                    if (inRange(row[rangeIndex], request)) {
                        result.add(project(row, projects));
                    }
                }
            }
        } else {
            for (final List<Object> prefix : batch.distinctPrefixes()) {
                for (final Object[] row : prefixGroup(rows, prefixLength, prefix)) {
                    result.add(project(row, projects));
                }
            }
        }
        return result;
    }

    /** Converts the pending buffer elements once per setData. */
    private List<Object[]> rows() {
        if (rows == null) {
            final List<Object[]> converted = new ArrayList<>();
            if (pending != null) {
                for (final MElement element : pending) {
                    if (element != null) {
                        converted.add(CalciteValues.toInternalRow(tableSchema, element));
                    }
                }
            }
            rows = converted;
            pending = null;
        }
        return rows;
    }

    /**
     * Rows whose leading {@code prefixLength} key columns equal {@code prefix},
     * from a hash index built lazily per prefix length. Rows with a null in an
     * indexed key column are excluded (SQL equality never matches null), as are
     * requests carrying a null component.
     */
    private List<Object[]> prefixGroup(List<Object[]> rows, int prefixLength,
            List<Object> prefix) {
        if (prefixLength == 0) {
            return rows;
        }
        // Immutable lists reject contains(null); iterate instead.
        for (final Object component : prefix) {
            if (component == null) {
                return List.of();
            }
        }
        final Map<List<Object>, List<Object[]>> index = indexes
                .computeIfAbsent(prefixLength, length -> buildIndex(rows, length));
        return index.getOrDefault(prefix, List.of());
    }

    private Map<List<Object>, List<Object[]>> buildIndex(List<Object[]> rows, int prefixLength) {
        final Map<List<Object>, List<Object[]>> index = new HashMap<>();
        outer:
        for (final Object[] row : rows) {
            final List<Object> key = new ArrayList<>(prefixLength);
            for (int i = 0; i < prefixLength; i++) {
                final Object value = row[keyIndexes[i]];
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

    // Indexed rows are shared across lookups and must not be mutated by callers.
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
        throw new IllegalArgumentException("field not found in buffer schema: " + fieldName);
    }
}
