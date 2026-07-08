package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupJoinRule;
import com.mercari.solution.util.pipeline.lookup.LookupLateralJoin;
import com.mercari.solution.util.pipeline.lookup.LookupLateralJoinRule;
import com.mercari.solution.util.pipeline.lookup.LookupLateralRuntime;
import com.mercari.solution.util.pipeline.lookup.LookupSchema;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.pipeline.udf.UserDefinedFunctions;
import com.mercari.solution.util.schema.CalciteSchemaUtil;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Linq4j;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.hep.HepPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.Hook;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScannableTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Statistic;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlExplainLevel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlValidator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Frameworks;
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
import java.util.Properties;
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
    private final List<UserDefinedFunctions.FunctionSpec> functions;
    private final Schema outputSchema;

    private transient PreparedStatement statement;
    private transient Map<String, List<MElement>> elements;
    private transient List<LookupLateralRuntime> lateralRuntimes;

    private Query2(
            final Map<String, Schema> inputSchemas,
            final List<LookupSource> sources,
            final List<UserDefinedFunctions.FunctionSpec> functions,
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
        this.functions = functions == null ? List.of() : List.copyOf(functions);
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
        return new Query2(inputSchemas, List.of(), List.of(), sql);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Assembles inputs, lookup sources and the SQL; {@link #build()} plans and validates. */
    public static class Builder {

        private final Map<String, Schema> inputSchemas = new LinkedHashMap<>();
        private final List<LookupSource> sources = new ArrayList<>();
        private final List<UserDefinedFunctions.FunctionSpec> functions = new ArrayList<>();
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

        /**
         * Registers a scalar UDF: every public static overload of
         * {@code clazz.methodName}. Use an UPPERCASE name (see
         * {@link UserDefinedFunctions}).
         */
        public Builder withScalarFunction(final String name, final Class<?> clazz,
                final String methodName) {
            this.functions.add(UserDefinedFunctions.scalar(name, clazz, methodName));
            return this;
        }

        /**
         * Registers an aggregate UDF (UDAF): {@code clazz} follows Calcite's
         * accumulator convention — {@code A init()}, {@code A add(A, V...)},
         * optional {@code A merge(A, A)}, {@code R result(A)}.
         */
        public Builder withAggregateFunction(final String name, final Class<?> clazz) {
            this.functions.add(UserDefinedFunctions.aggregate(name, clazz));
            return this;
        }

        public Builder withSql(final String sql) {
            this.sql = sql;
            return this;
        }

        public Query2 build() {
            return new Query2(inputSchemas, sources, functions, sql);
        }
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    /** Plans the SQL (with sources set up) and reads the result schema, without executing. */
    private Schema deriveOutputSchema() {
        final List<LookupLateralRuntime> runtimes = new ArrayList<>();
        try {
            setupSources();
            final Map<String, List<MElement>> buffers = createBuffers();
            try (final PreparedStatement s = prepare(createRootSchema(buffers), runtimes)) {
                return CalciteSchemaUtil.convertSchema(s.getMetaData());
            }
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalArgumentException("failed to plan sql: " + sql, e);
        } finally {
            closeLateralRuntimes(runtimes);
            closeSources();
        }
    }

    /** Prepares the statement once per worker. Call from {@code DoFn @Setup}. */
    public void setup() {
        setupSources();
        this.elements = createBuffers();
        this.lateralRuntimes = new ArrayList<>();
        try {
            this.statement = prepare(createRootSchema(this.elements), this.lateralRuntimes);
        } catch (final Throwable e) {
            closeLateralRuntimes(this.lateralRuntimes);
            this.lateralRuntimes = null;
            closeSources();
            throw new IllegalArgumentException("failed to prepare query: " + sql, e);
        }
    }

    /** Releases the statement, lateral evaluators and source clients. Call from {@code DoFn @Teardown}. */
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
        if (this.lateralRuntimes != null) {
            closeLateralRuntimes(this.lateralRuntimes);
            this.lateralRuntimes = null;
        }
        closeSources();
    }

    private static void closeLateralRuntimes(final List<LookupLateralRuntime> runtimes) {
        for (final LookupLateralRuntime runtime : runtimes) {
            try {
                runtime.close();
            } catch (final Throwable e) {
                LOG.error("failed to close lateral runtime", e);
            }
        }
        runtimes.clear();
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

    private SchemaPlus createRootSchema(final Map<String, List<MElement>> buffers) {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus defaultSchema =
                rootSchema.add("DefaultSchema", new InputSchema(inputSchemas, buffers));
        for (final Map.Entry<String, List<org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function>> entry
                : UserDefinedFunctions.build(functions).entrySet()) {
            for (final var function : entry.getValue()) {
                defaultSchema.add(entry.getKey(), function);
            }
        }
        for (final LookupSource source : sources) {
            rootSchema.add(source.getName(), LookupSchema.of(source));
        }
        return rootSchema;
    }

    /**
     * Parses, validates, converts and prepares the SQL.
     *
     * <p>The SQL→rel front-end is hand-assembled (rather than the Frameworks
     * {@code Planner}) so that <b>no decorrelation runs before our rules</b>:
     * {@code PlannerImpl.rel} unconditionally decorrelates, which rewrites
     * correlated LATERAL blocks over lookup tables into shapes that can no
     * longer be answered by key-driven reads (value-generator joins that scan
     * the external table). Here {@link LookupLateralJoinRule} claims those
     * {@code Correlate}s first, in a Hep pre-pass; any remaining correlations
     * (e.g. {@code UNNEST} over input arrays) are decorrelated later by the
     * runner's standard program, as before.
     *
     * <p>The plain lookup-join rules of every registered source are installed
     * both on the rel cluster's planner and via the thread-local
     * {@code Hook.PLANNER} (covering whichever planner the runner uses),
     * keeping concurrent instances isolated.
     */
    private PreparedStatement prepare(final SchemaPlus rootSchema,
            final List<LookupLateralRuntime> runtimes) throws Exception {

        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        final Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        final CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
        final CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema), List.of("DefaultSchema"),
                typeFactory, connectionConfig);

        // Standard operators + the BigQuery library (ARRAY_AGG etc. — matching
        // the BigQuery lex) + schema-registered UDFs via the catalog reader.
        final SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlOperatorTables.chain(
                        SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                                SqlLibrary.STANDARD, SqlLibrary.BIG_QUERY),
                        catalogReader),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));

        final SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setCaseSensitive(false)
                .setLex(Lex.BIG_QUERY)
                .build();
        final SqlNode parsed = SqlParser.create(sql, parserConfig).parseStmt();
        final SqlNode validated = validator.validate(parsed);

        final VolcanoPlanner volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptUtil.registerDefaultRules(volcano, false, false);
        final RelOptCluster cluster = RelOptCluster.create(volcano, new RexBuilder(typeFactory));

        final SqlToRelConverter converter = new SqlToRelConverter(
                noopViewExpander(), validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config().withTrimUnusedFields(false).withExpand(false));
        RelRoot root = converter.convertQuery(validated, false, true);
        // Deliberately NOT flattenTypes(root.rel, true) (PlannerImpl does; stock
        // JDBC prepare does not): the flattener rewrites any reference to a
        // nested ROW column of a lookup table into a field-access Project
        // directly over the scan, which LookupJoinRule cannot claim — the join
        // degrades to the rejected standalone scan. The enumerable runtime
        // executes unflattened RexFieldAccess fine (the origin engine runs
        // without flattening).
        RelNode relNode = root.project();

        if (LOG.isDebugEnabled()) {
            LOG.debug("query plan for sql: {}\n{}", sql,
                    RelOptUtil.toString(relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        if (sources.isEmpty()) {
            return RelRunners.run(relNode);
        }

        // Hep pre-pass: claim correlated LATERAL blocks over lookup tables
        // before any decorrelation can rewrite them.
        final HepProgramBuilder hepProgram = new HepProgramBuilder();
        for (final LookupSource source : sources) {
            hepProgram.addRuleInstance(new LookupLateralJoinRule(source, runtimes, functions));
        }
        final HepPlanner hepPlanner = new HepPlanner(hepProgram.build());
        hepPlanner.setRoot(relNode);
        relNode = hepPlanner.findBestExp();

        final List<RelOptRule> rules = new ArrayList<>();
        for (final LookupSource source : sources) {
            rules.add(new LookupJoinRule(source, false));
            rules.add(new LookupJoinRule(source, true));
        }
        rules.add(LookupLateralJoin.CONVERTER_RULE);
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

    private static RelOptTable.ViewExpander noopViewExpander() {
        return (rowType, queryString, schemaPath, viewPath) -> {
            throw new UnsupportedOperationException("views are not supported");
        };
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
