package com.mercari.solution.util.pipeline.lookup;

import com.mercari.solution.module.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.CorrelationId;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalSort;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCorrelVariable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexFieldAccess;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexShuttle;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexSubQuery;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexVisitorImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rewrites a correlated LATERAL block over a {@link LookupSource} table into a
 * {@link LookupLateralJoin}: the block's correlated conjuncts must constrain a
 * contiguous prefix of one of the table's candidate keys (point equality,
 * prefix-only equality for sources that support it, or prefix + bounded range —
 * the same contract as the plain lookup-join), they are turned into the
 * key-driven fetch, and the <em>rest of the block's plan</em> (aggregation,
 * uncorrelated filters, projections, ORDER BY / LIMIT) is carried as SQL and
 * evaluated per key set inside the DoFn.
 *
 * <pre>{@code
 * SELECT i.userId, s.total
 * FROM INPUT AS i
 * JOIN LATERAL (
 *   SELECT SUM(e.amount) AS total
 *   FROM db.EVENTS AS e
 *   WHERE e.USER_ID = i.userId AND e.SEQ >= 1 AND e.SEQ <= i.maxSeq
 * ) AS s ON TRUE
 * }</pre>
 *
 * <p>Must run in a Hep pre-pass <em>before any decorrelation</em> (which would
 * rewrite these correlates into shapes that scan the external table). Matched
 * shapes: the block is a single chain of Project / Filter / Aggregate / Sort
 * over one scan of this source's table; correlated conditions sit in filters
 * directly above the scan; the correlation appears nowhere else. INNER and
 * LEFT ({@code ON TRUE}) correlates only. Non-matching correlates are left
 * untouched (and will be rejected at execution if they would scan the table).
 */
public final class LookupLateralJoinRule extends RelOptRule {

    private final LookupSource source;
    private final List<LookupLateralRuntime> runtimes;
    private final List<com.mercari.solution.util.pipeline.udf.UserDefinedFunctions.FunctionSpec> functions;

    /**
     * @param runtimes  collector for the created per-block evaluators; the caller
     *                  owns their lifecycle and must close them on teardown
     * @param functions UDFs to register in the per-block evaluator (so the block
     *                  may reference them)
     */
    public LookupLateralJoinRule(LookupSource source, List<LookupLateralRuntime> runtimes,
            List<com.mercari.solution.util.pipeline.udf.UserDefinedFunctions.FunctionSpec> functions) {
        super(operand(LogicalCorrelate.class, any()),
                "LookupLateralJoinRule:" + source.getName());
        this.source = source;
        this.runtimes = runtimes;
        this.functions = functions;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalCorrelate correlate = call.rel(0);
        final JoinRelType joinType = correlate.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            return;
        }

        // The block must be a single Project/Filter/Aggregate/Sort chain over
        // one scan of this source's table.
        final List<RelNode> chain = new ArrayList<>();
        final TableScan scan = descendChain(unwrap(correlate.getRight()), chain);
        if (scan == null) {
            return;
        }
        final List<String> qualifiedName = scan.getTable().getQualifiedName();
        if (qualifiedName.size() < 2 || !qualifiedName.get(qualifiedName.size() - 2)
                .equalsIgnoreCase(source.getName())) {
            return;
        }
        final String tableName = qualifiedName.get(qualifiedName.size() - 1);
        final Schema leafSchema = source.tableSchemas().get(tableName);
        if (leafSchema == null) {
            return;
        }

        // Classify conditions: correlated conjuncts (allowed only in filters
        // directly above the scan) and scan-level literal conjuncts may form the
        // key contract; everything else stays in the block. Correlation anywhere
        // else (projections, sort bounds, filters above an aggregate) declines.
        final CorrelationId corId = correlate.getCorrelationId();
        final List<RexNode> corConjuncts = new ArrayList<>();
        final List<RexNode> literalConjuncts = new ArrayList<>();
        for (int i = 0; i < chain.size(); i++) {
            final RelNode node = chain.get(i);
            final boolean scanLevel = isScanLevel(chain, i);
            switch (node) {
                case LogicalFilter filter -> {
                    for (final RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
                        if (usesCorrelation(conjunct)) {
                            if (!scanLevel) {
                                return;
                            }
                            corConjuncts.add(conjunct);
                        } else if (scanLevel) {
                            literalConjuncts.add(conjunct);
                        }
                    }
                }
                case LogicalProject project -> {
                    for (final RexNode expr : project.getProjects()) {
                        if (usesCorrelation(expr)) {
                            return;
                        }
                    }
                }
                case LogicalSort sort -> {
                    if ((sort.fetch != null && usesCorrelation(sort.fetch))
                            || (sort.offset != null && usesCorrelation(sort.offset))) {
                        return;
                    }
                }
                default -> {
                }
            }
        }

        final RelDataType scanRowType = scan.getRowType();
        final int leftCount = unwrap(correlate.getLeft()).getRowType().getFieldCount();

        // Try each candidate key (primary key first); for each, first try to
        // consume the scan-level literal conjuncts into the contract too, then
        // with the correlated conjuncts alone (literals stay in the block).
        for (final LookupKey candidate : source.keyCandidates(tableName)) {
            final int[] keyLeafIndex = new int[candidate.columns().size()];
            boolean usable = true;
            for (int i = 0; i < keyLeafIndex.length; i++) {
                keyLeafIndex[i] = scanRowType.getFieldNames().indexOf(candidate.columns().get(i));
                if (keyLeafIndex[i] < 0) {
                    usable = false;
                    break;
                }
            }
            if (!usable) {
                continue;
            }
            Analysis analysis =
                    analyze(corConjuncts, literalConjuncts, keyLeafIndex, corId);
            if (analysis == null) {
                analysis = analyze(corConjuncts, List.of(), keyLeafIndex, corId);
            }
            if (analysis == null) {
                continue;
            }
            transform(call, correlate, chain, scan, tableName, leafSchema, candidate,
                    keyLeafIndex, analysis, leftCount, joinType == JoinRelType.LEFT);
            return;
        }
    }

    private void transform(RelOptRuleCall call, LogicalCorrelate correlate, List<RelNode> chain,
            TableScan scan, String tableName, Schema leafSchema, LookupKey candidate,
            int[] keyLeafIndex, Analysis analysis, int leftCount, boolean leftJoin) {

        final RelBuilder builder = call.builder();
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final RelDataType scanRowType = scan.getRowType();
        final int prefixLength = analysis.prefixExprs.size();

        // Append the translated key (and range bound) expressions to the left
        // input, cast to the leaf key column types.
        builder.push(unwrap(correlate.getLeft()));
        final List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < leftCount; i++) {
            projects.add(builder.field(i));
        }
        for (int i = 0; i < prefixLength; i++) {
            projects.add(rexBuilder.makeCast(
                    nullable(rexBuilder, scanRowType.getFieldList()
                            .get(keyLeafIndex[i]).getType()),
                    translateCorAccess(analysis.prefixExprs.get(i), rexBuilder)));
        }
        if (analysis.range) {
            final RelDataType rangeType = nullable(rexBuilder,
                    scanRowType.getFieldList().get(keyLeafIndex[prefixLength]).getType());
            projects.add(rexBuilder.makeCast(rangeType,
                    translateCorAccess(analysis.lowerExpr, rexBuilder)));
            projects.add(rexBuilder.makeCast(rangeType,
                    translateCorAccess(analysis.upperExpr, rexBuilder)));
        }
        builder.project(projects);
        final RelNode leftWithKeys = builder.build();

        // Strip the consumed conjuncts from the block (they are satisfied by the
        // key-driven fetch) and carry the remaining plan as SQL.
        final RelNode stripped = rebuild(unwrap(correlate.getRight()),
                analysis.consumed, rexBuilder);
        final String innerSql = new RelToSqlConverter(CalciteSqlDialect.DEFAULT)
                .visitRoot(stripped).asStatement()
                .toSqlString(CalciteSqlDialect.DEFAULT).getSql();
        final List<String> innerColumnTypes = new ArrayList<>();
        for (final RelDataTypeField field : stripped.getRowType().getFieldList()) {
            innerColumnTypes.add(field.getType().getSqlTypeName().name());
        }

        final LookupLateralRuntime runtime = LookupLateralRuntime.create(
                source.getName(), tableName, leafSchema, innerSql, innerColumnTypes, functions);
        runtimes.add(runtime);

        final int[] leafKeyPos = new int[analysis.range ? prefixLength + 1 : prefixLength];
        System.arraycopy(keyLeafIndex, 0, leafKeyPos, 0, leafKeyPos.length);

        call.transformTo(new LookupLateralJoin(
                correlate.getCluster(),
                correlate.getCluster().traitSetOf(Convention.NONE),
                leftWithKeys,
                correlate.getRowType(),
                source.lookupSourceId(),
                runtime.id(),
                tableName,
                candidate.indexName(),
                analysis.range,
                leftCount,
                prefixLength,
                leftCount + prefixLength,       // lower ordinal
                leftCount + prefixLength + 1,   // upper ordinal
                analysis.lowerInclusive,
                analysis.upperInclusive,
                csv(leafKeyPos),
                stripped.getRowType().getFieldCount(),
                leftJoin));
    }

    /** Result of condition analysis: the fetch shape and the consumed conjuncts. */
    private static final class Analysis {
        final List<RexNode> prefixExprs = new ArrayList<>();
        boolean range;
        RexNode lowerExpr;
        boolean lowerInclusive;
        RexNode upperExpr;
        boolean upperInclusive;
        final Set<RexNode> consumed =
                java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    }

    /**
     * Applies the key-prefix contract to the given conjuncts. All correlated
     * conjuncts must participate (else the fetch would miss their constraint —
     * residual correlations are not supported); literal conjuncts participate
     * as offered and the caller may retry without them.
     */
    private Analysis analyze(List<RexNode> corConjuncts, List<RexNode> literalConjuncts,
            int[] keyLeafIndex, CorrelationId corId) {
        final int keyCount = keyLeafIndex.length;
        final RexNode[] eq = new RexNode[keyCount];
        final RexNode[] lower = new RexNode[keyCount];
        final boolean[] lowerInc = new boolean[keyCount];
        final RexNode[] upper = new RexNode[keyCount];
        final boolean[] upperInc = new boolean[keyCount];
        final RexNode[] eqSource = new RexNode[keyCount];
        final RexNode[] lowerSource = new RexNode[keyCount];
        final RexNode[] upperSource = new RexNode[keyCount];

        for (int pass = 0; pass < 2; pass++) {
            final List<RexNode> conjuncts = pass == 0 ? corConjuncts : literalConjuncts;
            final boolean required = pass == 0;
            for (final RexNode conjunct : conjuncts) {
                if (!(conjunct instanceof RexCall callNode) || callNode.operands.size() != 2) {
                    if (required) {
                        return null;
                    }
                    continue;
                }
                final RexNode a = callNode.operands.get(0);
                final RexNode b = callNode.operands.get(1);
                SqlKind kind = callNode.getKind();
                int keyCol = keyColumnOf(a, keyLeafIndex);
                RexNode boundExpr;
                if (keyCol >= 0) {
                    boundExpr = b;
                } else {
                    keyCol = keyColumnOf(b, keyLeafIndex);
                    if (keyCol < 0) {
                        if (required) {
                            return null;
                        }
                        continue;
                    }
                    boundExpr = a;
                    kind = kind.reverse();
                }
                if (!boundReferencesOnlyCorrelation(boundExpr, corId)) {
                    if (required) {
                        return null;
                    }
                    continue;
                }
                switch (kind) {
                    case EQUALS -> {
                        if (eq[keyCol] != null || lower[keyCol] != null || upper[keyCol] != null) {
                            return null;
                        }
                        eq[keyCol] = boundExpr;
                        eqSource[keyCol] = conjunct;
                    }
                    case GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                        if (eq[keyCol] != null || lower[keyCol] != null) {
                            return null;
                        }
                        lower[keyCol] = boundExpr;
                        lowerInc[keyCol] = kind == SqlKind.GREATER_THAN_OR_EQUAL;
                        lowerSource[keyCol] = conjunct;
                    }
                    case LESS_THAN, LESS_THAN_OR_EQUAL -> {
                        if (eq[keyCol] != null || upper[keyCol] != null) {
                            return null;
                        }
                        upper[keyCol] = boundExpr;
                        upperInc[keyCol] = kind == SqlKind.LESS_THAN_OR_EQUAL;
                        upperSource[keyCol] = conjunct;
                    }
                    default -> {
                        if (required) {
                            return null;
                        }
                    }
                }
            }
        }

        // Equality must cover a contiguous prefix [0..M-1]; column M may carry a
        // bounded range (or nothing, for prefix-supporting sources); columns
        // after M must be unconstrained.
        int m = 0;
        while (m < keyCount && eq[m] != null) {
            m++;
        }
        final Analysis analysis = new Analysis();
        for (int i = 0; i < m; i++) {
            analysis.prefixExprs.add(eq[i]);
            analysis.consumed.add(eqSource[i]);
        }
        if (m == keyCount) {
            for (int i = 0; i < keyCount; i++) {
                if (lower[i] != null || upper[i] != null) {
                    return null;
                }
            }
            analysis.range = false;
            return analysis;
        }
        for (int i = m + 1; i < keyCount; i++) {
            if (eq[i] != null || lower[i] != null || upper[i] != null) {
                return null;
            }
        }
        if (lower[m] == null && upper[m] == null) {
            if (m == 0 || !source.supportsKeyPrefixLookup()) {
                return null;
            }
            analysis.range = false;
            return analysis;
        }
        if (lower[m] == null || upper[m] == null) {
            return null;
        }
        analysis.range = true;
        analysis.lowerExpr = lower[m];
        analysis.lowerInclusive = lowerInc[m];
        analysis.upperExpr = upper[m];
        analysis.upperInclusive = upperInc[m];
        analysis.consumed.add(lowerSource[m]);
        analysis.consumed.add(upperSource[m]);
        return analysis;
    }

    /** Rebuilds the block without the consumed conjuncts (and without Hep vertices). */
    private static RelNode rebuild(RelNode node, Set<RexNode> consumed, RexBuilder rexBuilder) {
        final RelNode current = unwrap(node);
        if (current instanceof TableScan) {
            return current;
        }
        final RelNode input = rebuild(current.getInput(0), consumed, rexBuilder);
        if (current instanceof LogicalFilter filter) {
            final List<RexNode> kept = new ArrayList<>();
            for (final RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
                if (!consumed.contains(conjunct)) {
                    kept.add(conjunct);
                }
            }
            if (kept.isEmpty()) {
                return input;
            }
            return LogicalFilter.create(input,
                    RexUtil.composeConjunction(rexBuilder, kept));
        }
        return current.copy(current.getTraitSet(), List.of(input));
    }

    /** Descends a Project/Filter/Aggregate/Sort chain to its single scan leaf. */
    private static TableScan descendChain(RelNode node, List<RelNode> chain) {
        RelNode current = node;
        while (true) {
            if (current instanceof TableScan scan) {
                return scan;
            }
            if (current instanceof LogicalProject || current instanceof LogicalFilter
                    || current instanceof LogicalAggregate || current instanceof LogicalSort) {
                chain.add(current);
                current = unwrap(current.getInput(0));
            } else {
                return null;
            }
        }
    }

    /** Whether everything below {@code chain[index]} down to the scan is filters only. */
    private static boolean isScanLevel(List<RelNode> chain, int index) {
        for (int i = index + 1; i < chain.size(); i++) {
            if (!(chain.get(i) instanceof LogicalFilter)) {
                return false;
            }
        }
        return true;
    }

    private static RelNode unwrap(RelNode node) {
        return node instanceof HepRelVertex vertex ? vertex.getCurrentRel() : node;
    }

    private static boolean usesCorrelation(RexNode node) {
        final boolean[] uses = {false};
        node.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                    uses[0] = true;
                }
                return super.visitFieldAccess(fieldAccess);
            }

            @Override
            public Void visitSubQuery(RexSubQuery subQuery) {
                uses[0] = true;
                return null;
            }
        });
        return uses[0];
    }

    /**
     * Whether the bound expression references only this correlate's variable
     * (any mix of correlation field accesses and literals; never leaf columns).
     */
    private static boolean boundReferencesOnlyCorrelation(RexNode node, CorrelationId corId) {
        final boolean[] ok = {true};
        node.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                ok[0] = false;
                return null;
            }

            @Override
            public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable variable) {
                    if (!variable.id.equals(corId)) {
                        ok[0] = false;
                    }
                    return null;
                }
                return super.visitFieldAccess(fieldAccess);
            }

            @Override
            public Void visitSubQuery(RexSubQuery subQuery) {
                ok[0] = false;
                return null;
            }
        });
        return ok[0];
    }

    /** Correlation field accesses → left input refs (the cor row type is the left row type). */
    private static RexNode translateCorAccess(RexNode node, RexBuilder rexBuilder) {
        return node.accept(new RexShuttle() {
            @Override
            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                    return rexBuilder.makeInputRef(
                            fieldAccess.getType(), fieldAccess.getField().getIndex());
                }
                return super.visitFieldAccess(fieldAccess);
            }
        });
    }

    /** Key position of a leaf column reference (possibly CAST-wrapped), or -1. */
    private static int keyColumnOf(RexNode node, int[] keyLeafIndex) {
        RexNode current = node;
        while (current instanceof RexCall call && call.getKind() == SqlKind.CAST
                && call.operands.size() == 1) {
            current = call.operands.get(0);
        }
        if (current instanceof RexInputRef ref) {
            for (int i = 0; i < keyLeafIndex.length; i++) {
                if (ref.getIndex() == keyLeafIndex[i]) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static RelDataType nullable(RexBuilder rexBuilder, RelDataType type) {
        return rexBuilder.getTypeFactory().createTypeWithNullability(type, true);
    }

    private static String csv(int[] array) {
        return java.util.Arrays.stream(array)
                .mapToObj(Integer::toString).collect(Collectors.joining(","));
    }
}
