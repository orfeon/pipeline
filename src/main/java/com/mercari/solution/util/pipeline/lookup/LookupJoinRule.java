package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexVisitorImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a join of the in-memory input to a {@link LookupSource} table on its
 * key into a {@link LookupJoin} (batched key-driven read). Bound to one source;
 * matches a join whose right side is a scan of a table in that source's schema,
 * and whose condition constrains a contiguous prefix of the table's key columns:
 *
 * <ul>
 *   <li><b>Point</b>: equality on every key column ({@code key = leftExpr}).</li>
 *   <li><b>Range</b>: equality on the leading key columns plus a bounded range
 *       on the next key column.</li>
 * </ul>
 *
 * <p>Supports INNER and LEFT joins. Matches a bare scan and a column-pruning
 * {@code Project} over a scan. Any other condition shape leaves the join alone
 * (which will fail at execution with the scan-rejection message, since lookup
 * tables cannot be scanned).
 */
public final class LookupJoinRule extends RelOptRule {

    private final LookupSource source;
    private final boolean withProject;

    public LookupJoinRule(LookupSource source, boolean withProject) {
        super(withProject
                        ? operand(LogicalJoin.class,
                                operand(RelNode.class, any()),
                                operand(LogicalProject.class,
                                        operand(TableScan.class, none())))
                        : operand(LogicalJoin.class,
                                operand(RelNode.class, any()),
                                operand(TableScan.class, none())),
                "LookupJoinRule:" + source.getName() + (withProject ? ":project" : ":scan"));
        this.source = source;
        this.withProject = withProject;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        LogicalProject project = withProject ? call.rel(2) : null;
        TableScan scan = withProject ? call.rel(3) : call.rel(2);

        List<String> qualifiedName = scan.getTable().getQualifiedName();
        // Qualified names are [schemaName, tableName]; the parser may be
        // case-insensitive, so compare the schema part ignoring case.
        if (qualifiedName.size() < 2
                || !qualifiedName.get(qualifiedName.size() - 2).equalsIgnoreCase(source.getName())) {
            return;
        }
        String tableName = qualifiedName.get(qualifiedName.size() - 1);
        List<LookupKey> candidates = source.keyCandidates(tableName);
        if (candidates.isEmpty()) {
            return;
        }
        JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            return;
        }

        RelDataType scanRowType = scan.getRowType();
        int[] rightProjects = resolveProjects(project, scanRowType.getFieldCount());
        if (rightProjects == null) {
            return;
        }
        int leftCount = join.getLeft().getRowType().getFieldCount();
        List<RexNode> conjuncts = RelOptUtil.conjunctions(join.getCondition());

        // Try each candidate key (primary key first); use the first the join
        // condition matches as a point or prefix+range.
        for (LookupKey candidate : candidates) {
            List<String> keyFields = candidate.columns();
            int keyCount = keyFields.size();
            int[] keyProjPos = new int[keyCount];
            RelDataType[] keyTypes = new RelDataType[keyCount];
            int[] keyGlobalIndex = new int[keyCount];
            boolean usable = true;
            for (int i = 0; i < keyCount; i++) {
                int scanIndex = scanRowType.getFieldNames().indexOf(keyFields.get(i));
                if (scanIndex < 0) {
                    usable = false;
                    break;
                }
                // A key column pruned from the projection (by the field trimmer)
                // cannot appear in the join condition, so it can only end up
                // beyond the matched prefix — mark it unmatchable instead of
                // rejecting the whole key.
                int projPos = indexOf(rightProjects, scanIndex);
                keyProjPos[i] = projPos;
                keyTypes[i] = scanRowType.getFieldList().get(scanIndex).getType();
                keyGlobalIndex[i] = projPos < 0 ? -1 : leftCount + projPos;
            }
            if (!usable) {
                continue;
            }
            Analysis analysis = analyze(conjuncts, keyGlobalIndex, leftCount,
                    source.supportsKeyPrefixLookup());
            if (analysis == null) {
                continue;
            }
            call.transformTo(buildLookupJoin(call, join, tableName, candidate.indexName(),
                    analysis, keyTypes, keyProjPos, rightProjects, leftCount,
                    joinType == JoinRelType.LEFT));
            return;
        }
    }

    private LookupJoin buildLookupJoin(RelOptRuleCall call, LogicalJoin join, String tableName,
            String indexName, Analysis analysis, RelDataType[] keyTypes, int[] keyProjPos,
            int[] rightProjects, int leftCount, boolean leftJoin) {
        // Append the prefix (and range bound) expressions to the left input,
        // cast to the key column types so left/right values compare equal.
        RelBuilder builder = call.builder();
        RexBuilder rexBuilder = builder.getRexBuilder();
        builder.push(join.getLeft());
        List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < leftCount; i++) {
            projects.add(builder.field(i));
        }
        int prefixLength = analysis.prefixExprs.size();
        for (int i = 0; i < prefixLength; i++) {
            projects.add(rexBuilder.makeCast(
                    nullable(rexBuilder, keyTypes[i]), analysis.prefixExprs.get(i)));
        }
        if (analysis.range) {
            RelDataType rangeType = keyTypes[prefixLength];
            projects.add(rexBuilder.makeCast(
                    nullable(rexBuilder, rangeType), analysis.lowerExpr));
            projects.add(rexBuilder.makeCast(
                    nullable(rexBuilder, rangeType), analysis.upperExpr));
        }
        builder.project(projects);
        RelNode leftWithKeys = builder.build();

        RelNode enumLeft = convert(leftWithKeys,
                leftWithKeys.getTraitSet().replace(EnumerableConvention.INSTANCE));

        return new LookupJoin(
                join.getCluster(),
                join.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
                enumLeft,
                join.getRowType(),
                source.lookupSourceId(),
                tableName,
                indexName,
                analysis.range,
                leftCount,
                prefixLength,
                leftCount + prefixLength,       // lower ordinal
                leftCount + prefixLength + 1,   // upper ordinal
                analysis.lowerInclusive,
                analysis.upperInclusive,
                csv(keyProjPos, prefixLength),
                analysis.range ? keyProjPos[prefixLength] : -1,
                rightProjects.length,
                csv(rightProjects, rightProjects.length),
                leftJoin);
    }

    private static RelDataType nullable(RexBuilder rexBuilder, RelDataType type) {
        return rexBuilder.getTypeFactory().createTypeWithNullability(type, true);
    }

    /** Result of condition analysis. */
    private static final class Analysis {
        final List<RexNode> prefixExprs = new ArrayList<>();
        boolean range;
        RexNode lowerExpr;
        boolean lowerInclusive;
        RexNode upperExpr;
        boolean upperInclusive;
    }

    private Analysis analyze(List<RexNode> conjuncts, int[] keyGlobalIndex, int leftCount,
            boolean allowPrefixOnly) {
        int keyCount = keyGlobalIndex.length;
        RexNode[] eq = new RexNode[keyCount];
        RexNode[] lower = new RexNode[keyCount];
        boolean[] lowerInc = new boolean[keyCount];
        RexNode[] upper = new RexNode[keyCount];
        boolean[] upperInc = new boolean[keyCount];

        for (RexNode conjunct : conjuncts) {
            if (!(conjunct instanceof RexCall callNode) || callNode.operands.size() != 2) {
                return null;
            }
            RexNode a = callNode.operands.get(0);
            RexNode b = callNode.operands.get(1);
            SqlKind kind = callNode.getKind();
            int keyCol = keyColumnOf(a, keyGlobalIndex);
            RexNode boundExpr;
            if (keyCol >= 0) {
                boundExpr = b;
            } else {
                keyCol = keyColumnOf(b, keyGlobalIndex);
                if (keyCol < 0) {
                    return null;
                }
                boundExpr = a;
                kind = kind.reverse();
            }
            if (!refsOnlyLeft(boundExpr, leftCount)) {
                return null;
            }
            switch (kind) {
                case EQUALS:
                    if (eq[keyCol] != null || lower[keyCol] != null || upper[keyCol] != null) {
                        return null;
                    }
                    eq[keyCol] = boundExpr;
                    break;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (eq[keyCol] != null || lower[keyCol] != null) {
                        return null;
                    }
                    lower[keyCol] = boundExpr;
                    lowerInc[keyCol] = kind == SqlKind.GREATER_THAN_OR_EQUAL;
                    break;
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    if (eq[keyCol] != null || upper[keyCol] != null) {
                        return null;
                    }
                    upper[keyCol] = boundExpr;
                    upperInc[keyCol] = kind == SqlKind.LESS_THAN_OR_EQUAL;
                    break;
                default:
                    return null;
            }
        }

        // Equality must cover a contiguous prefix [0..M-1]; column M may carry a
        // bounded range; columns after M must be unconstrained.
        int m = 0;
        while (m < keyCount && eq[m] != null) {
            m++;
        }
        Analysis analysis = new Analysis();
        for (int i = 0; i < m; i++) {
            analysis.prefixExprs.add(eq[i]);
        }
        if (m == keyCount) {
            // Pure point lookup on the full key.
            for (int i = 0; i < keyCount; i++) {
                if (lower[i] != null || upper[i] != null) {
                    return null;
                }
            }
            analysis.range = false;
            return analysis;
        }
        // Later columns must be unconstrained; no stray equality past the prefix.
        for (int i = m + 1; i < keyCount; i++) {
            if (eq[i] != null || lower[i] != null || upper[i] != null) {
                return null;
            }
        }
        if (lower[m] == null && upper[m] == null) {
            // Equality on a strict leading prefix, nothing else: an index-backed
            // prefix lookup, for sources that support it.
            if (m == 0 || !allowPrefixOnly) {
                return null;
            }
            analysis.range = false;
            return analysis;
        }
        // Otherwise column m must carry a bounded range.
        if (lower[m] == null || upper[m] == null) {
            return null;
        }
        analysis.range = true;
        analysis.lowerExpr = lower[m];
        analysis.lowerInclusive = lowerInc[m];
        analysis.upperExpr = upper[m];
        analysis.upperInclusive = upperInc[m];
        return analysis;
    }

    private static int keyColumnOf(RexNode node, int[] keyGlobalIndex) {
        if (node instanceof RexInputRef ref) {
            for (int i = 0; i < keyGlobalIndex.length; i++) {
                if (ref.getIndex() == keyGlobalIndex[i]) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static boolean refsOnlyLeft(RexNode node, int leftCount) {
        boolean[] ok = {true};
        node.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                if (inputRef.getIndex() >= leftCount) {
                    ok[0] = false;
                }
                return null;
            }
        });
        return ok[0];
    }

    private static int[] resolveProjects(LogicalProject project, int scanFieldCount) {
        if (project == null) {
            int[] all = new int[scanFieldCount];
            for (int i = 0; i < scanFieldCount; i++) {
                all[i] = i;
            }
            return all;
        }
        List<RexNode> exprs = project.getProjects();
        int[] ordinals = new int[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            int ref = underlyingColumn(exprs.get(i));
            if (ref < 0) {
                return null; // not a simple column (or cast of one) projection
            }
            ordinals[i] = ref;
        }
        return ordinals;
    }

    /** Scan column index of a projection expr that is a column ref or a cast of one. */
    private static int underlyingColumn(RexNode expr) {
        RexNode node = expr;
        while (node instanceof RexCall call && call.getKind() == SqlKind.CAST
                && call.operands.size() == 1) {
            node = call.operands.get(0);
        }
        return node instanceof RexInputRef ref ? ref.getIndex() : -1;
    }

    private static int indexOf(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return i;
            }
        }
        return -1;
    }

    private static String csv(int[] array, int length) {
        return Arrays.stream(array, 0, length)
                .mapToObj(Integer::toString).collect(Collectors.joining(","));
    }
}
