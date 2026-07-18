package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.QueryProvider;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Blocks;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelShuttle;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.TimeFrames;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.Bindable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScannableTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A relational plan compiled once into a directly-executable {@link Bindable},
 * evaluated repeatedly without the JDBC layer.
 *
 * <p>The per-element SQL engine previously re-executed a prepared Avatica
 * statement per evaluation, at a fixed ~35µs per call regardless of query
 * complexity. Measurement traced the cost not to the statement machinery
 * itself but to the scan implementation the JDBC runner picks:
 * {@code RelRunners} rewrites scans of scannable tables into
 * {@code BindableTableScan}s, which plan to an {@code EnumerableInterpreter}
 * whose generated {@code bind()} constructs an {@code Interpreter} —
 * including a Hep optimization pass over the scan subtree — on <em>every</em>
 * execution. This class runs the same logical pipeline
 * ({@code Programs.standard()}: sub-query removal, decorrelation, field
 * trimming, the volcano phase, calc rewrite) but rewrites the scans to
 * {@link InternalEnumerableScan}s — direct {@code Schemas.enumerable(table,
 * root)} calls (possible because this {@link DataContext} exposes the root
 * schema) with no per-field conversion — and compiles the physical plan to a
 * {@link Bindable} once. Per evaluation it only binds a fresh context and
 * drains the enumerable — roughly 40x less per-evaluation overhead.
 *
 * <p>Values are produced in Calcite-internal representation (epoch-millis
 * timestamps, epoch-day dates, {@code List} arrays, {@code Object[]} rows) —
 * no Avatica local-wallclock rendering to invert.
 */
public final class BindableQuery {

    private final Bindable<?> bindable;
    private final RelDataType rowType;
    private final RelNode physicalPlan;
    private final SchemaPlus rootSchema;
    private final JavaTypeFactory typeFactory;
    // Objects the code generator stashed for runtime retrieval (e.g. the
    // RelNode an EnumerableInterpreter re-reads via DataContext.get).
    private final Map<String, Object> internalParameters;
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);
    private final Object timeFrameSet;

    private BindableQuery(Bindable<?> bindable, RelNode physicalPlan, SchemaPlus rootSchema,
            JavaTypeFactory typeFactory, Map<String, Object> internalParameters) {
        this.bindable = bindable;
        this.rowType = physicalPlan.getRowType();
        this.physicalPlan = physicalPlan;
        this.rootSchema = rootSchema;
        this.typeFactory = typeFactory;
        this.internalParameters = internalParameters;
        this.timeFrameSet = typeFactory.getTypeSystem().deriveTimeFrameSet(TimeFrames.CORE);
    }

    /**
     * Physicalizes and compiles {@code relNode} on its own cluster's planner.
     * The planner must already hold any source-specific rules (the lookup-join
     * rules); the default logical/enumerable rules are ensured here (idempotent).
     *
     * @param relNode    the logical plan (after any Hep pre-pass)
     * @param rootSchema the root schema the plan's table scans resolve against
     */
    public static BindableQuery compile(final RelNode relNode, final SchemaPlus rootSchema) {
        // Unlike RelRunners we do NOT rewrite scans into BindableTableScans
        // (that path pays a per-execution Interpreter, see the class comment).
        // We also cannot leave them to EnumerableTableScan: for a scannable
        // table with an array-of-ROW column it generates a per-field
        // conversion into synthesized record classes that neither compiles
        // (untyped Object[] rows) nor matches this engine's Calcite-internal
        // value representation. InternalEnumerableScan emits the raw
        // Schemas.enumerable(table, root) call with no conversion — our scan
        // rows are already in internal form.
        final RelShuttle shuttle = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(TableScan scan) {
                if (scan instanceof LogicalTableScan
                        && scan.getTable().unwrap(ScannableTable.class) != null) {
                    return new InternalEnumerableScan(scan.getCluster(), scan.getTable());
                }
                return super.visit(scan);
            }
        };
        final RelNode rel = relNode.accept(shuttle);
        final RelOptPlanner planner = rel.getCluster().getPlanner();
        // The JDBC runner's planner carries the default rule set; ensure ours
        // does too (VolcanoPlanner.addRule ignores duplicates), since e.g. the
        // lateral runtime's Frameworks planner starts without them.
        RelOptUtil.registerDefaultRules(planner, false, false);
        final RelTraitSet desired = rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
        final RelNode physical = Programs.standard()
                .run(planner, rel, desired, List.of(), List.of());
        // The code generator stashes runtime objects into this map (an
        // EnumerableInterpreter's RelNode, for one) and the generated bind()
        // reads them back through the DataContext — keep it and serve it.
        final Map<String, Object> internalParameters = new HashMap<>();
        final Bindable<?> bindable = EnumerableInterpretable.toBindable(
                internalParameters, null, (EnumerableRel) physical, EnumerableRel.Prefer.ARRAY);
        return new BindableQuery(bindable, physical, rootSchema,
                (JavaTypeFactory) rel.getCluster().getTypeFactory(), internalParameters);
    }

    /** The physical result row type (column names and SQL types). */
    public RelDataType rowType() {
        return rowType;
    }

    /**
     * The optimized physical plan this query executes — the input for plan
     * inspections (e.g. verifying every lookup-table access became a
     * key-driven read).
     */
    public RelNode physicalPlan() {
        return physicalPlan;
    }

    /**
     * Executes the plan against the current table contents and returns the
     * result rows as full-width {@code Object[]} in Calcite-internal values.
     */
    public List<Object[]> executeInternalRows() {
        return executeInternalRows(0, 0);
    }

    /**
     * Executes with runtime guards checked at every row boundary: the result
     * row cap and the evaluation deadline. Both use distinctive
     * {@code "query guard:"} messages so violations are recognizable in
     * failure records. A blocked call inside a lookup source is not
     * interrupted — the deadline fires at the next row boundary.
     *
     * @param maxRows       maximum result rows (0 = unlimited)
     * @param deadlineNanos absolute {@link System#nanoTime()} deadline (0 = none)
     */
    public List<Object[]> executeInternalRows(final long maxRows, final long deadlineNanos) {
        final DataContext context = createDataContext();
        final int fieldCount = rowType.getFieldCount();
        final List<Object[]> rows = new ArrayList<>();
        for (final Object row : bindable.bind(context)) {
            if (deadlineNanos != 0 && System.nanoTime() - deadlineNanos >= 0) {
                throw new IllegalStateException(
                        "query guard: evaluation exceeded the timeoutMillis deadline");
            }
            if (maxRows > 0 && rows.size() >= maxRows) {
                throw new IllegalStateException(
                        "query guard: evaluation produced more than maxRows=" + maxRows
                                + " result rows");
            }
            rows.add(toArray(row, fieldCount));
        }
        return rows;
    }

    // The enumerable's row format for a single-column result may be the bare
    // value or a one-element array depending on the chosen PhysType; normalize
    // to an array. (A single column of ROW type whose struct has exactly one
    // field is ambiguous here; such results are not produced by this engine's
    // supported output shapes.)
    private static Object[] toArray(final Object row, final int fieldCount) {
        if (fieldCount == 1) {
            if (row instanceof Object[] array && array.length == 1) {
                return array;
            }
            return new Object[]{row};
        }
        return (Object[]) row;
    }

    /**
     * A physical scan of one of this engine's scannable tables: compiles to a
     * direct {@code Schemas.enumerable(table, root)} call with an untyped
     * {@code Object[]} row format and <b>no per-field conversion</b> — the
     * tables' scan rows are already Calcite-internal values.
     */
    private static final class InternalEnumerableScan extends TableScan implements EnumerableRel {

        private InternalEnumerableScan(
                final org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster cluster,
                final org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptTable table) {
            super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), List.of(), table);
        }

        @Override
        public RelNode copy(
                final org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet traitSet,
                final List<RelNode> inputs) {
            return new InternalEnumerableScan(getCluster(), table);
        }

        @Override
        public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
            // optimize=false: the scan always emits Object[] rows, even for a
            // single-column table (the optimizer would flip the declared
            // format to SCALAR and downstream casts would break).
            final PhysType physType = PhysTypeImpl.of(
                    implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
            // getExpression(ScannableTable.class) already yields the full
            // Schemas.enumerable(table, root) call expression.
            final Expression call = table.getExpression(ScannableTable.class);
            return implementor.result(physType, Blocks.toBlock(call));
        }
    }

    /**
     * A fresh context per evaluation, mirroring the JDBC connection's
     * {@code DataContextImpl}: the standard time/locale variables (read by
     * {@code CURRENT_TIMESTAMP} etc.) recomputed per execution, the root
     * schema for table resolution, and a cancel flag.
     */
    private DataContext createDataContext() {
        final long time = System.currentTimeMillis();
        final TimeZone timeZone = TimeZone.getDefault();
        final long offset = timeZone.getOffset(time);
        final Map<String, Object> variables = new HashMap<>();
        variables.put(DataContext.Variable.UTC_TIMESTAMP.camelName, time);
        variables.put(DataContext.Variable.CURRENT_TIMESTAMP.camelName, time + offset);
        variables.put(DataContext.Variable.LOCAL_TIMESTAMP.camelName, time + offset);
        variables.put(DataContext.Variable.TIME_ZONE.camelName, timeZone);
        variables.put(DataContext.Variable.LOCALE.camelName, Locale.ROOT);
        variables.put(DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag);
        variables.put(DataContext.Variable.TIME_FRAME_SET.camelName, timeFrameSet);
        variables.put(DataContext.Variable.USER.camelName, "sa");
        variables.put(DataContext.Variable.SYSTEM_USER.camelName,
                System.getProperty("user.name"));
        return new DataContext() {
            @Override
            public SchemaPlus getRootSchema() {
                return rootSchema;
            }

            @Override
            public JavaTypeFactory getTypeFactory() {
                return typeFactory;
            }

            @Override
            public QueryProvider getQueryProvider() {
                // Only Queryable-based scans need a query provider; this
                // engine's tables are all scannable/bindable.
                return null;
            }

            @Override
            public Object get(String name) {
                final Object variable = variables.get(name);
                if (variable != null) {
                    return variable;
                }
                return internalParameters.get(name);
            }
        };
    }
}
