package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expressions;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Physical operator joining the in-memory input to an external
 * {@link LookupSource} by its key, via batched key-driven reads (see
 * {@link LookupJoinEnumerable}). Source-agnostic: the source is referenced by
 * its registry id, so the planner never scans the external table in full.
 */
public final class LookupJoin extends SingleRel implements EnumerableRel {

    private final RelDataType outputRowType;
    private final long sourceId;
    private final String table;
    private final String indexName;
    private final boolean range;
    private final int leftFieldCount;
    private final int prefixLength;
    private final int lowerOrdinal;
    private final int upperOrdinal;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;
    private final String rightPrefixPos;
    private final int rightRangePos;
    private final int rightFieldCount;
    private final String rightProjects;
    private final boolean leftJoin;

    public LookupJoin(RelOptCluster cluster, RelTraitSet traits, RelNode input,
            RelDataType outputRowType, long sourceId, String table, String indexName,
            boolean range, int leftFieldCount, int prefixLength, int lowerOrdinal,
            int upperOrdinal, boolean lowerInclusive, boolean upperInclusive,
            String rightPrefixPos, int rightRangePos, int rightFieldCount, String rightProjects,
            boolean leftJoin) {
        super(cluster, traits, input);
        this.outputRowType = outputRowType;
        this.sourceId = sourceId;
        this.table = table;
        this.indexName = indexName;
        this.range = range;
        this.leftFieldCount = leftFieldCount;
        this.prefixLength = prefixLength;
        this.lowerOrdinal = lowerOrdinal;
        this.upperOrdinal = upperOrdinal;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.rightPrefixPos = rightPrefixPos;
        this.rightRangePos = rightRangePos;
        this.rightFieldCount = rightFieldCount;
        this.rightProjects = rightProjects;
        this.leftJoin = leftJoin;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LookupJoin(getCluster(), traitSet, inputs.get(0), outputRowType, sourceId,
                table, indexName, range, leftFieldCount, prefixLength, lowerOrdinal, upperOrdinal,
                lowerInclusive, upperInclusive, rightPrefixPos, rightRangePos, rightFieldCount,
                rightProjects, leftJoin);
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Cheap: one batched key read driven by the (small) left input.
        double rows = mq.getRowCount(getInput());
        return planner.getCostFactory().makeCost(rows, rows, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("source", sourceId)
                .item("table", table)
                .itemIf("index", indexName, indexName != null)
                .item("lookup", range ? "range" : "point")
                .item("joinType", leftJoin ? "left" : "inner");
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        BlockBuilder builder = new BlockBuilder();
        Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
        PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY);
        Expression leftEnumerable = builder.append("left", leftResult.block);
        Expression lookup = Expressions.new_(
                LookupJoinEnumerable.class,
                leftEnumerable,
                Expressions.constant(sourceId, long.class),
                Expressions.constant(table),
                Expressions.constant(indexName, String.class),
                Expressions.constant(range),
                Expressions.constant(leftFieldCount),
                Expressions.constant(prefixLength),
                Expressions.constant(lowerOrdinal),
                Expressions.constant(upperOrdinal),
                Expressions.constant(lowerInclusive),
                Expressions.constant(upperInclusive),
                Expressions.constant(rightPrefixPos),
                Expressions.constant(rightRangePos),
                Expressions.constant(rightFieldCount),
                Expressions.constant(rightProjects),
                Expressions.constant(leftJoin));
        builder.add(Expressions.return_(null, lookup));
        return implementor.result(physType, builder.toBlock());
    }
}
