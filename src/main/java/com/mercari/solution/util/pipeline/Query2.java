package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupJoinRule;
import com.mercari.solution.util.pipeline.lookup.LookupSchema;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.schema.CalciteSchemaUtil;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Linq4j;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.Hook;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScannableTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Statistic;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlExplainLevel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Planner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelRunners;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Executes a Calcite SQL statement over bounded, in-memory {@link MElement} lists,
 * optionally joined against external {@link LookupSource}s (JDBC / Spanner /
 * Bigtable / REST) via <em>key-driven lookup-joins</em>.
 *
 * <p>This is the successor of {@link Query}: the same "SQL inside a DoFn" core
 * (plan and compile once per worker, evaluate repeatedly against small in-memory
 * inputs, no shuffle, windowing/timestamps untouched), self-contained (it does not
 * depend on the deprecated {@code util.domain.sql.calcite} package), plus external
 * source support:
 *
 * <ul>
 *   <li>Register a source with {@link Builder#withSource}; its tables are
 *       referenced in SQL as {@code sourceName.tableName}.</li>
 *   <li>A join of an input table to a lookup table whose condition constrains a
 *       contiguous prefix of the table's key (point equality on the full key, or
 *       leading-column equality + a bounded range on the next column) is rewritten
 *       to a batched key-driven read — the external table is never scanned.
 *       INNER and LEFT joins are supported.</li>
 *   <li>Any other use of a lookup table (a standalone scan, an unsupported join
 *       condition) fails at execution with an explanatory error.</li>
 * </ul>
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Construct via {@link #of}/{@link #builder()} at pipeline construction time —
 *       this sets up the sources (deriving external table metadata once), validates
 *       the SQL and derives {@link #getOutputSchema()} without executing, then
 *       releases the sources' clients.</li>
 *   <li>The instance is {@link Serializable} (derived metadata included); ship it
 *       to workers inside a {@code DoFn}.</li>
 *   <li>Call {@link #setup()} in {@code @Setup} — re-opens source clients, plans and
 *       prepares the statement once.</li>
 *   <li>Call {@link #execute} per element/bundle as often as needed.</li>
 *   <li>Call {@link #teardown()} in {@code @Teardown}.</li>
 * </ol>
 *
 * <p>Instances are not thread-safe after {@link #setup()}; use one instance per
 * DoFn instance (Beam guarantees a DoFn instance is not called concurrently).
 */
public class Query2 implements Serializable {

    private static final String DEFAULT_TABLE_NAME = "INPUT";
    private static final Logger LOG = LoggerFactory.getLogger(Query2.class);

    private final String sql;
    private final Map<String, Schema> inputSchemas;
    private final List<LookupSource> sources;
    private final Schema outputSchema;

    private transient Planner planner;
    private transient PreparedStatement statement;
    private transient Map<String, List<MElement>> elements;

    private Query2(
            final Map<String, Schema> inputSchemas,
            final List<LookupSource> sources,
            final String sql) {

        if (sql == null || sql.isEmpty()) {
            throw new IllegalArgumentException("sql must not be null or empty");
        }
        if (inputSchemas == null || inputSchemas.isEmpty()) {
            throw new IllegalArgumentException("at least one input schema is required");
        }
        this.sql = sql;
        this.inputSchemas = new LinkedHashMap<>(inputSchemas);
        this.sources = sources == null ? List.of() : List.copyOf(sources);
        this.outputSchema = deriveOutputSchema();
    }

    public static Query2 of(final String name, final Schema inputSchema, final String sql) {
        return builder().withInput(name, inputSchema).withSql(sql).build();
    }

    public static Query2 of(
            final String name,
            final List<Schema.Field> inputFields,
            final String sql) {

        return of(name, Schema.of(inputFields), sql);
    }

    public static Query2 of(final Map<String, Schema> inputSchemas, final String sql) {
        return new Query2(inputSchemas, List.of(), sql);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Assembles inputs, lookup sources and the SQL; {@link #build()} plans and validates. */
    public static class Builder {

        private final Map<String, Schema> inputSchemas = new LinkedHashMap<>();
        private final List<LookupSource> sources = new ArrayList<>();
        private String sql;

        public Builder withInput(final String name, final Schema schema) {
            this.inputSchemas.put(name, schema);
            return this;
        }

        public Builder withSource(final LookupSource source) {
            for (final LookupSource existing : sources) {
                if (existing.getName().equalsIgnoreCase(source.getName())) {
                    throw new IllegalArgumentException(
                            "duplicate lookup source name: " + source.getName());
                }
            }
            this.sources.add(source);
            return this;
        }

        public Builder withSources(final List<? extends LookupSource> sources) {
            for (final LookupSource source : sources) {
                withSource(source);
            }
            return this;
        }

        public Builder withSql(final String sql) {
            this.sql = sql;
            return this;
        }

        public Query2 build() {
            return new Query2(inputSchemas, sources, sql);
        }
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    /** Plans the SQL (with sources set up) and reads the result schema, without executing. */
    private Schema deriveOutputSchema() {
        try {
            setupSources();
            final Map<String, List<MElement>> buffers = createBuffers();
            try (final Planner p = createPlanner(buffers);
                 final PreparedStatement s = prepare(p, sql)) {
                return CalciteSchemaUtil.convertSchema(s.getMetaData());
            }
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalArgumentException("failed to plan sql: " + sql, e);
        } finally {
            closeSources();
        }
    }

    /** Prepares the statement once per worker. Call from {@code DoFn @Setup}. */
    public void setup() {
        setupSources();
        this.elements = createBuffers();
        this.planner = createPlanner(this.elements);
        try {
            this.statement = prepare(this.planner, sql);
        } catch (final Throwable e) {
            closeSources();
            throw new IllegalArgumentException("failed to prepare query: " + sql, e);
        }
    }

    /** Releases the statement, planner and source clients. Call from {@code DoFn @Teardown}. */
    public void teardown() {
        try {
            if (this.statement != null && !this.statement.isClosed()) {
                this.statement.close();
            }
        } catch (final SQLException e) {
            LOG.error("failed to close statement for sql: {}", sql, e);
        } finally {
            this.statement = null;
        }
        try {
            if (this.planner != null) {
                this.planner.close();
            }
        } catch (final Throwable e) {
            LOG.error("failed to close planner for sql: {}", sql, e);
        } finally {
            this.planner = null;
        }
        closeSources();
    }

    public List<MElement> execute(final MElement input, final Instant timestamp) {
        return execute(List.of(input), timestamp);
    }

    public List<MElement> execute(final List<MElement> inputs, final Instant timestamp) {
        // When a single table is registered, feed the inputs to it under its
        // registered name, falling back to the default table name.
        final String tableName;
        if (inputSchemas.size() == 1) {
            tableName = inputSchemas.keySet().iterator().next();
        } else {
            tableName = DEFAULT_TABLE_NAME;
        }
        return execute(Map.of(tableName, inputs), timestamp);
    }

    /**
     * Evaluates the prepared statement against the given per-table inputs.
     * Table names must match the schema names given at construction; the outputs
     * are {@code DataType.ELEMENT} rows carrying the given timestamp as event time.
     */
    public List<MElement> execute(final Map<String, List<MElement>> inputs, final Instant timestamp) {
        if (statement == null) {
            throw new IllegalStateException("query is not set up (call setup() first)");
        }
        for (final Map.Entry<String, List<MElement>> entry : inputs.entrySet()) {
            final List<MElement> buffer = elements.get(entry.getKey());
            if (buffer != null) {
                buffer.clear();
                buffer.addAll(entry.getValue());
            }
        }
        try (final ResultSet resultSet = statement.executeQuery()) {
            final List<Map<String, Object>> valuesList = CalciteValues.convertResultSet(resultSet);
            return MElement.ofList(valuesList, timestamp);
        } catch (final SQLException e) {
            throw new IllegalStateException("failed to execute query: " + sql, e);
        }
    }

    private void setupSources() {
        for (final LookupSource source : sources) {
            source.setup();
        }
    }

    private void closeSources() {
        for (final LookupSource source : sources) {
            try {
                source.close();
            } catch (final Throwable e) {
                LOG.error("failed to close lookup source: {}", source.getName(), e);
            }
        }
    }

    private Map<String, List<MElement>> createBuffers() {
        final Map<String, List<MElement>> buffers = new HashMap<>();
        for (final String name : inputSchemas.keySet()) {
            buffers.put(name, new ArrayList<>());
        }
        return buffers;
    }

    private Planner createPlanner(final Map<String, List<MElement>> buffers) {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus defaultSchema =
                rootSchema.add("DefaultSchema", new InputSchema(inputSchemas, buffers));
        for (final LookupSource source : sources) {
            rootSchema.add(source.getName(), LookupSchema.of(source));
        }
        final SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setCaseSensitive(false)
                .setLex(Lex.BIG_QUERY)
                .build();
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(defaultSchema)
                .build();
        return Frameworks.getPlanner(config);
    }

    /**
     * Parses, validates and prepares the SQL. The lookup-join rules of every
     * registered source are installed both on the rel cluster's planner and via
     * the thread-local {@code Hook.PLANNER} (covering whichever planner the
     * runner uses), keeping concurrent instances isolated.
     */
    private PreparedStatement prepare(final Planner p, final String sql) throws Exception {
        final SqlNode parsed = p.parse(sql);
        final SqlNode validated = p.validate(parsed);
        final RelNode relNode = p.rel(validated).project();

        if (LOG.isDebugEnabled()) {
            LOG.debug("query plan for sql: {}\n{}", sql,
                    RelOptUtil.toString(relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        final List<RelOptRule> rules = new ArrayList<>();
        for (final LookupSource source : sources) {
            rules.add(new LookupJoinRule(source, false));
            rules.add(new LookupJoinRule(source, true));
        }
        if (rules.isEmpty()) {
            return RelRunners.run(relNode);
        }
        for (final RelOptRule rule : rules) {
            relNode.getCluster().getPlanner().addRule(rule);
        }
        final Consumer<RelOptPlanner> registrar = plannerToConfigure -> {
            for (final RelOptRule rule : rules) {
                plannerToConfigure.addRule(rule);
            }
        };
        try (Hook.Closeable ignored = Hook.PLANNER.addThread(registrar)) {
            return RelRunners.run(relNode);
        }
    }

    /** Calcite schema holding the mutable in-memory input tables. */
    private static class InputSchema extends AbstractSchema {

        private final Map<String, Table> tableMap;

        private InputSchema(
                final Map<String, Schema> schemas,
                final Map<String, List<MElement>> buffers) {

            this.tableMap = new LinkedHashMap<>();
            for (final Map.Entry<String, Schema> entry : schemas.entrySet()) {
                tableMap.put(entry.getKey(),
                        new InputTable(entry.getValue(), buffers.get(entry.getKey())));
            }
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return tableMap;
        }
    }

    /**
     * A mutable in-memory table over the current input elements. Scan rows use
     * Calcite-internal values (see {@link CalciteValues}) so that join keys
     * compare equal against lookup-source rows.
     */
    private static class InputTable extends AbstractTable implements ScannableTable {

        private static final double SMALL_ROW_COUNT = 100d;

        private final Schema schema;
        private final List<MElement> elements;

        private RelDataType rowType;

        private InputTable(final Schema schema, final List<MElement> elements) {
            this.schema = schema;
            this.elements = elements;
        }

        @Override
        public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
            if (this.rowType == null) {
                this.rowType = CalciteSchemaUtil.convertSchema(schema, typeFactory);
            }
            return this.rowType;
        }

        @Override
        public Statistic getStatistic() {
            // A small constant: the input side of a lookup-join is always the
            // per-element/per-bundle batch, vastly smaller than any lookup table.
            return new Statistic() {
                @Override
                public Double getRowCount() {
                    return SMALL_ROW_COUNT;
                }
            };
        }

        @Override
        public Enumerable<Object[]> scan(final DataContext root) {
            return Linq4j.asEnumerable(elements)
                    .select(element -> CalciteValues.toInternalRow(schema, element));
        }
    }
}
