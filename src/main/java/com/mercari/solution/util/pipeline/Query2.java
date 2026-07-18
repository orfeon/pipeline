package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.BindableQuery;
import com.mercari.solution.util.pipeline.lookup.LookupAccessValidator;
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
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelRecordType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.StructKind;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Executes Calcite SQL over bounded, in-memory {@link MElement} lists,
 * optionally joined against external {@link LookupSource}s (JDBC / Spanner /
 * Bigtable / Datastore / Firestore / REST / gRPC) via <em>key-driven lookup-joins</em>.
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
 * <p><b>Sessions</b>: an instance holds one or more named statements evaluated in
 * order per {@link #executeAll} call, all sharing the same input tables, lookup
 * sources (and their caches / lateral runtimes) and UDFs. Every statement's
 * result is additionally registered as an in-memory table under the statement's
 * name, so later statements can reference earlier results (CTE-like reuse — the
 * rows are passed as raw Calcite-internal values with no conversion). Statements
 * marked {@code output} contribute a named entry to the session result; with
 * {@code exclusive}, evaluation stops after the first output statement that
 * produced at least one row (non-output statements always run).
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Construct via {@link #of}/{@link #builder()} at pipeline construction time —
 *       this sets up the sources (deriving external table metadata once), validates
 *       the SQL and derives {@link #getOutputSchemas()} without executing, then
 *       releases the sources' clients.</li>
 *   <li>The instance is {@link Serializable} (derived metadata included); ship it
 *       to workers inside a {@code DoFn}.</li>
 *   <li>Call {@link #setup()} in {@code @Setup} — re-opens source clients, plans and
 *       compiles the statements once.</li>
 *   <li>Call {@link #execute}/{@link #executeAll} per element/bundle as often as
 *       needed.</li>
 *   <li>Call {@link #teardown()} in {@code @Teardown}.</li>
 * </ol>
 *
 * <p>Instances are not thread-safe after {@link #setup()}; use one instance per
 * DoFn instance (Beam guarantees a DoFn instance is not called concurrently).
 */
public class Query2 implements Serializable {

    private static final String DEFAULT_TABLE_NAME = "INPUT";
    private static final Logger LOG = LoggerFactory.getLogger(Query2.class);

    /**
     * One named statement of the session. {@code output} statements contribute
     * a named entry to the session result; a {@code BUFFER_INSERT} statement's
     * rows are returned separately (the host persists them into Beam state), a
     * {@code STATE_RESTORE} statement's rows likewise (the host seeds Beam
     * state from them on a key's first touch). Every statement's result is
     * referenceable as a table by later statements.
     */
    public record Statement(String name, String sql, boolean output, Kind kind)
            implements Serializable {

        /** What a statement's result feeds. */
        public enum Kind {
            QUERY, BUFFER_INSERT, STATE_RESTORE
        }

        public Statement {
            if (sql == null || sql.isEmpty()) {
                throw new IllegalArgumentException("statement sql must not be null or empty");
            }
            if (name == null) {
                name = "";
            }
            if (kind == null) {
                kind = Kind.QUERY;
            }
        }

        public static Statement of(String name, String sql, boolean output) {
            return new Statement(name, sql, output, Kind.QUERY);
        }
    }

    /**
     * The per-element result of a session: output name → rows, plus the
     * buffer-insert and state-restore statements' rows (null when the
     * statement is absent or was not evaluated).
     */
    public record SessionResult(
            Map<String, List<MElement>> outputs,
            List<MElement> bufferRows,
            List<MElement> restoreRows) {
    }

    /**
     * Runtime guards applied per evaluation, checked at every result-row
     * boundary: {@code maxRows} caps each statement's result rows and
     * {@code timeoutMillis} is a deadline over the whole {@link #executeAll}
     * call (0 = disabled). Violations throw with a {@code "query guard:"}
     * message and ride the host's normal failure handling (failFast /
     * failure sinks). A call blocked inside a lookup source is not
     * interrupted — the deadline fires at the next row boundary.
     */
    public record Guard(long maxRows, long timeoutMillis) implements Serializable {

        public Guard {
            if (maxRows < 0 || timeoutMillis < 0) {
                throw new IllegalArgumentException(
                        "guard maxRows and timeoutMillis must not be negative");
            }
        }

        /** True when neither guard is configured. */
        public boolean isNoop() {
            return maxRows == 0 && timeoutMillis == 0;
        }
    }

    private final List<Statement> statements;
    private final boolean exclusive;
    private final Map<String, Schema> inputSchemas;
    private final List<LookupSource> sources;
    private final List<UserDefinedFunctions.FunctionSpec> functions;
    private final Guard guard;                         // null when unconfigured
    private final Map<String, Schema> outputSchemas;   // output statements only
    private final Schema bufferInsertSchema;           // null when no BUFFER_INSERT statement
    private final Schema stateRestoreSchema;           // null when no STATE_RESTORE statement

    private transient List<CompiledStatement> compiled;
    private transient Map<String, List<MElement>> elements;
    private transient List<LookupLateralRuntime> lateralRuntimes;

    private static final class CompiledStatement {
        final Statement statement;
        final BindableQuery bindableQuery;
        final List<String> fieldNames;
        final IntermediateTable intermediate;

        CompiledStatement(Statement statement, BindableQuery bindableQuery,
                List<String> fieldNames, IntermediateTable intermediate) {
            this.statement = statement;
            this.bindableQuery = bindableQuery;
            this.fieldNames = fieldNames;
            this.intermediate = intermediate;
        }
    }

    private Query2(
            final Map<String, Schema> inputSchemas,
            final List<LookupSource> sources,
            final List<UserDefinedFunctions.FunctionSpec> functions,
            final List<Statement> statements,
            final boolean exclusive,
            final Guard guard) {

        if (statements == null || statements.isEmpty()) {
            throw new IllegalArgumentException("at least one statement (sql) is required");
        }
        if (inputSchemas == null || inputSchemas.isEmpty()) {
            throw new IllegalArgumentException("at least one input schema is required");
        }
        this.statements = List.copyOf(statements);
        this.exclusive = exclusive;
        this.inputSchemas = new LinkedHashMap<>(inputSchemas);
        this.sources = sources == null ? List.of() : List.copyOf(sources);
        this.functions = functions == null ? List.of() : List.copyOf(functions);
        this.guard = guard == null || guard.isNoop() ? null : guard;
        validateStatements();
        validateLookupAccess();
        // keyed by statement index: names may repeat across the special-kind
        // statements and the legacy unnamed single statement (all "")
        final Map<Integer, Schema> derivedSchemas = deriveOutputSchemas();
        this.outputSchemas = new LinkedHashMap<>();
        Schema insertSchema = null;
        Schema restoreSchema = null;
        for (int i = 0; i < this.statements.size(); i++) {
            final Statement statement = this.statements.get(i);
            final Schema schema = derivedSchemas.get(i);
            switch (statement.kind()) {
                case BUFFER_INSERT -> insertSchema = schema;
                case STATE_RESTORE -> restoreSchema = schema;
                case QUERY -> {
                    if (statement.output()) {
                        this.outputSchemas.put(statement.name(), schema);
                    }
                }
            }
        }
        this.bufferInsertSchema = insertSchema;
        this.stateRestoreSchema = restoreSchema;
    }

    private void validateStatements() {
        final Set<String> names = new HashSet<>();
        int outputCount = 0;
        int queryCount = 0;
        int bufferInsertCount = 0;
        int stateRestoreCount = 0;
        boolean sawQuery = false;
        for (final Statement statement : statements) {
            if (statement.kind() != Statement.Kind.QUERY) {
                if (statement.kind() == Statement.Kind.BUFFER_INSERT) {
                    bufferInsertCount++;
                } else {
                    stateRestoreCount++;
                }
                if (sawQuery) {
                    throw new IllegalArgumentException(
                            "special-kind statements must precede the query statements");
                }
                continue;
            }
            sawQuery = true;
            queryCount++;
            if (statement.output()) {
                outputCount++;
            }
            if (statement.name().isEmpty()) {
                continue;
            }
            if (!names.add(statement.name().toUpperCase())) {
                throw new IllegalArgumentException(
                        "duplicate statement name: " + statement.name());
            }
            for (final String inputName : inputSchemas.keySet()) {
                if (inputName.equalsIgnoreCase(statement.name())) {
                    throw new IllegalArgumentException(
                            "statement name '" + statement.name()
                                    + "' collides with an input table name");
                }
            }
            for (final LookupSource source : sources) {
                if (source.getName().equalsIgnoreCase(statement.name())) {
                    throw new IllegalArgumentException(
                            "statement name '" + statement.name()
                                    + "' collides with a lookup source name");
                }
            }
        }
        for (final Statement statement : statements) {
            if (statement.kind() == Statement.Kind.QUERY
                    && statement.name().isEmpty() && queryCount > 1) {
                throw new IllegalArgumentException(
                        "statements require a name when more than one is defined");
            }
        }
        if (bufferInsertCount > 1) {
            throw new IllegalArgumentException("at most one bufferInsert statement is allowed");
        }
        if (stateRestoreCount > 1) {
            throw new IllegalArgumentException("at most one stateRestore statement is allowed");
        }
        if (outputCount == 0) {
            throw new IllegalArgumentException("at least one output statement is required");
        }
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
        return new Query2(inputSchemas, List.of(), List.of(),
                List.of(Statement.of("", sql, true)), false, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Assembles inputs, lookup sources and the statements; {@link #build()} plans and validates. */
    public static class Builder {

        private final Map<String, Schema> inputSchemas = new LinkedHashMap<>();
        private final List<LookupSource> sources = new ArrayList<>();
        private final List<UserDefinedFunctions.FunctionSpec> functions = new ArrayList<>();
        private final List<Statement> statements = new ArrayList<>();
        private String bufferInsertSql;
        private String stateRestoreSql;
        private boolean exclusive;
        private Guard guard;

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

        /** The single-statement form: equivalent to one unnamed output statement. */
        public Builder withSql(final String sql) {
            return withQuery("", sql, true);
        }

        /** Appends a named statement (see {@link Statement}). */
        public Builder withQuery(final String name, final String sql, final boolean output) {
            this.statements.add(Statement.of(name, sql, output));
            return this;
        }

        public Builder withQueries(final List<Statement> statements) {
            this.statements.addAll(statements);
            return this;
        }

        /**
         * A statement whose result rows the host persists into the buffer state
         * (evaluated first, not an output). See the query module's buffer source.
         */
        public Builder withBufferInsert(final String sql) {
            this.bufferInsertSql = sql;
            return this;
        }

        /**
         * A statement whose result rows the host seeds Beam state from on a
         * key's first touch (not an output; must carry the buffered rows'
         * columns plus their original {@code __timestamp}). See the query
         * module's buffer source.
         */
        public Builder withStateRestore(final String sql) {
            this.stateRestoreSql = sql;
            return this;
        }

        /**
         * Stop evaluating after the first output statement that produced rows
         * (non-output statements always run).
         */
        public Builder withExclusive(final boolean exclusive) {
            this.exclusive = exclusive;
            return this;
        }

        /** Runtime guards applied per evaluation (see {@link Guard}). */
        public Builder withGuard(final Guard guard) {
            this.guard = guard;
            return this;
        }

        public Query2 build() {
            final List<Statement> all = new ArrayList<>();
            if (stateRestoreSql != null) {
                all.add(new Statement("", stateRestoreSql, false, Statement.Kind.STATE_RESTORE));
            }
            if (bufferInsertSql != null) {
                all.add(new Statement("", bufferInsertSql, false, Statement.Kind.BUFFER_INSERT));
            }
            all.addAll(statements);
            return new Query2(inputSchemas, sources, functions, all, exclusive, guard);
        }
    }

    /** The single output statement's schema (sessions with one output only). */
    public Schema getOutputSchema() {
        if (outputSchemas.size() != 1) {
            throw new IllegalStateException(
                    "the session has " + outputSchemas.size()
                            + " output statements; use getOutputSchemas()");
        }
        return outputSchemas.values().iterator().next();
    }

    /** Output statement name → result schema, in statement order. */
    public Map<String, Schema> getOutputSchemas() {
        return outputSchemas;
    }

    /** The bufferInsert statement's result schema, or null when none is defined. */
    public Schema getBufferInsertSchema() {
        return bufferInsertSchema;
    }

    /** The stateRestore statement's result schema, or null when none is defined. */
    public Schema getStateRestoreSchema() {
        return stateRestoreSchema;
    }

    /**
     * The registered lookup sources (immutable). Lets the hosting {@code DoFn}
     * reach sources that need per-element runtime state, e.g. feeding side input
     * contents to a {@code SideInputLookupSource} before {@link #execute}.
     */
    public List<LookupSource> getSources() {
        return sources;
    }

    /** A visitor over the sequentially planned statements. */
    private interface StatementVisitor {
        void visit(int index, Statement statement, Planned planned, SchemaPlus rootSchema,
                IntermediateTable intermediate) throws Exception;
    }

    /**
     * Plans the statements in order against a root schema that grows with each
     * statement's result table, so later statements can reference earlier ones.
     */
    private void forEachStatement(
            final Map<String, List<MElement>> buffers,
            final List<LookupLateralRuntime> runtimes,
            final StatementVisitor visitor) throws Exception {

        final Map<String, Table> intermediates = new LinkedHashMap<>();
        for (int i = 0; i < statements.size(); i++) {
            final Statement statement = statements.get(i);
            final SchemaPlus rootSchema = createRootSchema(buffers, intermediates);
            final Planned planned;
            try {
                planned = plan(rootSchema, runtimes, statement.sql());
            } catch (final Exception e) {
                throw new IllegalArgumentException(
                        "failed to plan query" + describeStatement(statement)
                                + ": " + statement.sql(), e);
            }
            final IntermediateTable intermediate =
                    IntermediateTable.of(planned.relNode().getRowType(), planned.fieldNames());
            visitor.visit(i, statement, planned, rootSchema, intermediate);
            if (!statement.name().isEmpty()) {
                intermediates.put(statement.name(), intermediate);
            }
        }
    }

    private static String describeStatement(final Statement statement) {
        return switch (statement.kind()) {
            case BUFFER_INSERT -> " (bufferInsert)";
            case STATE_RESTORE -> " (stateRestore)";
            case QUERY -> statement.name().isEmpty() ? "" : " '" + statement.name() + "'";
        };
    }

    /** Plans every statement (with sources set up) and reads the result schemas, without executing. */
    private Map<Integer, Schema> deriveOutputSchemas() {
        final List<LookupLateralRuntime> runtimes = new ArrayList<>();
        final Map<Integer, Schema> schemas = new LinkedHashMap<>();
        try {
            setupSources();
            final Map<String, List<MElement>> buffers = createBuffers();
            forEachStatement(buffers, runtimes, (index, statement, planned, rootSchema, intermediate) -> {
                if (statement.output() || statement.kind() != Statement.Kind.QUERY) {
                    try (final PreparedStatement s = prepare(planned)) {
                        schemas.put(index, CalciteSchemaUtil.convertSchema(s.getMetaData()));
                    }
                }
            });
            return schemas;
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalArgumentException("failed to plan sql", e);
        } finally {
            closeLateralRuntimes(runtimes);
            closeSources();
        }
    }

    /**
     * Plans and compiles every statement once at construction time and verifies
     * that each lookup-table access became a key-driven read. A join whose
     * condition does not fit the key contract leaves a scan of the lookup table
     * in the plan — that scan can never execute (it throws per element on the
     * workers), so it is rejected here, at submission time, with an error
     * naming the tables. Skipped when no lookup sources are registered.
     */
    private void validateLookupAccess() {
        if (sources.isEmpty()) {
            return;
        }
        final List<LookupLateralRuntime> runtimes = new ArrayList<>();
        try {
            setupSources();
            final Map<String, List<MElement>> buffers = createBuffers();
            forEachStatement(buffers, runtimes,
                    (index, statement, planned, rootSchema, intermediate) ->
                            LookupAccessValidator.validateKeyDrivenAccess(
                                    BindableQuery.compile(planned.relNode(), rootSchema)
                                            .physicalPlan(),
                                    statement.sql()));
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalArgumentException("failed to validate query plan", e);
        } finally {
            closeLateralRuntimes(runtimes);
            closeSources();
        }
    }

    /** Plans and compiles the statements once per worker. Call from {@code DoFn @Setup}. */
    public void setup() {
        setupSources();
        this.elements = createBuffers();
        this.lateralRuntimes = new ArrayList<>();
        final List<CompiledStatement> compiledStatements = new ArrayList<>();
        try {
            forEachStatement(this.elements, this.lateralRuntimes,
                    (index, statement, planned, rootSchema, intermediate) ->
                            compiledStatements.add(new CompiledStatement(
                                    statement,
                                    BindableQuery.compile(planned.relNode(), rootSchema),
                                    planned.fieldNames(),
                                    intermediate)));
            this.compiled = compiledStatements;
        } catch (final RuntimeException e) {
            closeLateralRuntimes(this.lateralRuntimes);
            this.lateralRuntimes = null;
            closeSources();
            throw e;
        } catch (final Throwable e) {
            closeLateralRuntimes(this.lateralRuntimes);
            this.lateralRuntimes = null;
            closeSources();
            throw new IllegalArgumentException("failed to prepare query", e);
        }
    }

    /** Releases the compiled plans, lateral evaluators and source clients. Call from {@code DoFn @Teardown}. */
    public void teardown() {
        this.compiled = null;
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
     * Evaluates the session and returns the <em>first</em> output statement's
     * rows — the single-statement form's result.
     */
    public List<MElement> execute(final Map<String, List<MElement>> inputs, final Instant timestamp) {
        final SessionResult result = executeAll(inputs, timestamp);
        if (result.outputs().isEmpty()) {
            return List.of();
        }
        return result.outputs().values().iterator().next();
    }

    /** Evaluates every statement of the session. */
    public SessionResult executeAll(final Map<String, List<MElement>> inputs, final Instant timestamp) {
        return executeAll(inputs, timestamp, java.util.EnumSet.allOf(Statement.Kind.class));
    }

    /**
     * Evaluates the session against the given per-table inputs. Statements of
     * the requested kinds run in order; every evaluated statement's rows are
     * registered under its name for later statements; {@code output} statements
     * contribute to the result map (in statement order). With {@code exclusive},
     * evaluation stops after the first output statement that produced rows.
     *
     * @param kinds which statement kinds to evaluate (the host separates the
     *        state-restore / persistence / output phases around its Beam state
     *        operations)
     */
    public SessionResult executeAll(
            final Map<String, List<MElement>> inputs,
            final Instant timestamp,
            final Set<Statement.Kind> kinds) {

        if (compiled == null) {
            throw new IllegalStateException("query is not set up (call setup() first)");
        }
        final long guardMaxRows = guard != null ? guard.maxRows() : 0;
        final long guardDeadlineNanos = guard != null && guard.timeoutMillis() > 0
                ? System.nanoTime() + guard.timeoutMillis() * 1_000_000L : 0;
        for (final Map.Entry<String, List<MElement>> entry : inputs.entrySet()) {
            final List<MElement> buffer = elements.get(entry.getKey());
            if (buffer != null) {
                buffer.clear();
                buffer.addAll(entry.getValue());
            }
        }
        final Map<String, List<MElement>> outputs = new LinkedHashMap<>();
        List<MElement> bufferRows = null;
        List<MElement> restoreRows = null;
        for (final CompiledStatement statement : compiled) {
            if (!kinds.contains(statement.statement.kind())) {
                continue;
            }
            try {
                final List<Object[]> rows = statement.bindableQuery
                        .executeInternalRows(guardMaxRows, guardDeadlineNanos);
                statement.intermediate.setRows(rows);
                switch (statement.statement.kind()) {
                    case BUFFER_INSERT -> bufferRows = toElements(statement, rows, timestamp);
                    case STATE_RESTORE -> restoreRows = toElements(statement, rows, timestamp);
                    case QUERY -> {
                        if (statement.statement.output()) {
                            final List<MElement> results = toElements(statement, rows, timestamp);
                            outputs.put(statement.statement.name(), results);
                            if (exclusive && !results.isEmpty()) {
                                return new SessionResult(outputs, bufferRows, restoreRows);
                            }
                        }
                    }
                }
            } catch (final RuntimeException e) {
                throw new IllegalStateException(
                        "failed to execute query" + describeStatement(statement.statement)
                                + ": " + statement.statement.sql(), e);
            }
        }
        return new SessionResult(outputs, bufferRows, restoreRows);
    }

    private static List<MElement> toElements(final CompiledStatement statement,
            final List<Object[]> rows, final Instant timestamp) {
        final List<Map<String, Object>> valuesList = CalciteValues.convertInternalRows(
                statement.fieldNames, statement.bindableQuery.rowType(), rows);
        return MElement.ofList(valuesList, timestamp);
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

    private SchemaPlus createRootSchema(
            final Map<String, List<MElement>> buffers,
            final Map<String, Table> intermediates) {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus defaultSchema =
                rootSchema.add("DefaultSchema", new InputSchema(inputSchemas, buffers, intermediates));
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
     * The logical plan, the source rules installed for its volcano phase, and
     * the SQL output field names (aliases) from the validated root.
     */
    private record Planned(RelNode relNode, List<RelOptRule> rules, List<String> fieldNames) {
    }

    /**
     * Prepares a planned statement as a JDBC statement — used only at
     * construction time to derive the output schema from the statement metadata
     * (workers execute through {@link BindableQuery} instead). The lookup rules
     * are installed both on the rel cluster's planner and via the thread-local
     * {@code Hook.PLANNER} (covering whichever planner the JDBC runner uses),
     * keeping concurrent instances isolated.
     */
    private PreparedStatement prepare(final Planned planned) throws Exception {
        if (planned.rules().isEmpty()) {
            return RelRunners.run(planned.relNode());
        }
        final Consumer<RelOptPlanner> registrar = plannerToConfigure -> {
            for (final RelOptRule rule : planned.rules()) {
                plannerToConfigure.addRule(rule);
            }
        };
        try (Hook.Closeable ignored = Hook.PLANNER.addThread(registrar)) {
            return RelRunners.run(planned.relNode());
        }
    }

    /**
     * Parses, validates and converts one statement, claims lookup LATERAL
     * blocks and installs the lookup rules on the cluster's planner.
     *
     * <p>The SQL→rel front-end is hand-assembled (rather than the Frameworks
     * {@code Planner}) so that <b>no decorrelation runs before our rules</b>:
     * {@code PlannerImpl.rel} unconditionally decorrelates, which rewrites
     * correlated LATERAL blocks over lookup tables into shapes that can no
     * longer be answered by key-driven reads (value-generator joins that scan
     * the external table). Here {@link LookupLateralJoinRule} claims those
     * {@code Correlate}s first, in a Hep pre-pass; any remaining correlations
     * (e.g. {@code UNNEST} over input arrays) are decorrelated later by the
     * standard program, as before.
     */
    private Planned plan(final SchemaPlus rootSchema,
            final List<LookupLateralRuntime> runtimes,
            final String sql) throws Exception {

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
        // The SQL output aliases live in RelRoot.fields, not (reliably) in the
        // physical row type — a trivial rename projection is removed by the
        // planner (ProjectRemoveRule ignores names). The JDBC layer renames at
        // the metadata level; the direct BindableQuery path applies these names
        // at result conversion.
        final List<String> fieldNames = new ArrayList<>();
        for (final var field : root.fields) {
            fieldNames.add(field.getValue());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("query plan for sql: {}\n{}", sql,
                    RelOptUtil.toString(relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        if (sources.isEmpty()) {
            return new Planned(relNode, List.of(), fieldNames);
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
        return new Planned(relNode, rules, fieldNames);
    }

    private static RelOptTable.ViewExpander noopViewExpander() {
        return (rowType, queryString, schemaPath, viewPath) -> {
            throw new UnsupportedOperationException("views are not supported");
        };
    }

    /** Calcite schema holding the mutable in-memory input and intermediate tables. */
    private static class InputSchema extends AbstractSchema {

        private final Map<String, Table> tableMap;

        private InputSchema(
                final Map<String, Schema> schemas,
                final Map<String, List<MElement>> buffers,
                final Map<String, Table> intermediates) {

            this.tableMap = new LinkedHashMap<>();
            for (final Map.Entry<String, Schema> entry : schemas.entrySet()) {
                tableMap.put(entry.getKey(),
                        new InputTable(entry.getValue(), buffers.get(entry.getKey())));
            }
            tableMap.putAll(intermediates);
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

    /**
     * A mutable in-memory table over one statement's current result rows
     * (raw Calcite-internal {@code Object[]}, no conversion), letting later
     * statements of the session reference earlier results by name. The row
     * type is captured from the producing statement's plan (with the SQL
     * output aliases applied) and re-materialized per planning type factory.
     */
    private static final class IntermediateTable extends AbstractTable implements ScannableTable {

        private static final double SMALL_ROW_COUNT = 100d;

        private final RelDataType sourceRowType;
        private final List<Object[]> rows = new ArrayList<>();

        private IntermediateTable(final RelDataType sourceRowType) {
            this.sourceRowType = sourceRowType;
        }

        private static IntermediateTable of(final RelDataType rowType, final List<String> fieldNames) {
            final List<RelDataTypeField> fields = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                final String name = i < fieldNames.size()
                        ? fieldNames.get(i) : rowType.getFieldList().get(i).getName();
                fields.add(new RelDataTypeFieldImpl(name, i,
                        rowType.getFieldList().get(i).getType()));
            }
            return new IntermediateTable(new RelRecordType(StructKind.PEEK_FIELDS, fields, true));
        }

        private void setRows(final List<Object[]> newRows) {
            rows.clear();
            rows.addAll(newRows);
        }

        @Override
        public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
            // The captured type belongs to the producing statement's type
            // factory; re-materialize it in the requesting one.
            return typeFactory.copyType(sourceRowType);
        }

        @Override
        public Statistic getStatistic() {
            return new Statistic() {
                @Override
                public Double getRowCount() {
                    return SMALL_ROW_COUNT;
                }
            };
        }

        @Override
        public Enumerable<Object[]> scan(final DataContext root) {
            return Linq4j.asEnumerable(rows);
        }
    }
}
