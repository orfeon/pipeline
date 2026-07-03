package com.mercari.solution.util.pipeline.lookup;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Expressions;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Physical operator for a correlated-LATERAL lookup join: fetches leaf rows
 * from a {@link LookupSource} by batched key-driven reads and evaluates the
 * LATERAL block's inner plan per key set inside the DoFn (see
 * {@link LookupLateralEnumerable}). Created by {@link LookupLateralJoinRule} in
 * a Hep pre-pass with {@link Convention#NONE}; {@link #CONVERTER_RULE} converts
 * it (and its input) to the enumerable convention during cost-based planning.
 */
public final class LookupLateralJoin extends SingleRel implements EnumerableRel {

    private final RelDataType outputRowType;
    private final long sourceId;
    private final long runtimeId;
    private final String table;
    private final String indexName;
    private final boolean range;
    private final int leftFieldCount;
    private final int prefixLength;
    private final int lowerOrdinal;
    private final int upperOrdinal;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;
    private final String leafKeyPos;
    private final int innerFieldCount;
    private final boolean leftJoin;

    public LookupLateralJoin(RelOptCluster cluster, RelTraitSet traits, RelNode input,
            RelDataType outputRowType, long sourceId, long runtimeId, String table,
            String indexName, boolean range, int leftFieldCount, int prefixLength,
            int lowerOrdinal, int upperOrdinal, boolean lowerInclusive, boolean upperInclusive,
            String leafKeyPos, int innerFieldCount, boolean leftJoin) {
        super(cluster, traits, input);
        this.outputRowType = outputRowType;
        this.sourceId = sourceId;
        this.runtimeId = runtimeId;
        this.table = table;
        this.indexName = indexName;
        this.range = range;
        this.leftFieldCount = leftFieldCount;
        this.prefixLength = prefixLength;
        this.lowerOrdinal = lowerOrdinal;
        this.upperOrdinal = upperOrdinal;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.leafKeyPos = leafKeyPos;
        this.innerFieldCount = innerFieldCount;
        this.leftJoin = leftJoin;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LookupLateralJoin(getCluster(), traitSet, inputs.get(0), outputRowType,
                sourceId, runtimeId, table, indexName, range, leftFieldCount, prefixLength,
                lowerOrdinal, upperOrdinal, lowerInclusive, upperInclusive, leafKeyPos,
                innerFieldCount, leftJoin);
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Cheap: one batched key read + bounded in-memory evaluation per key.
        final double rows = mq.getRowCount(getInput());
        return planner.getCostFactory().makeCost(rows, rows, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("source", sourceId)
                .item("table", table)
                .itemIf("index", indexName, indexName != null)
                .item("lookup", range ? "range" : "point")
                .item("lateral", runtimeId)
                .item("joinType", leftJoin ? "left" : "inner");
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY);
        final Expression leftEnumerable = builder.append("left", leftResult.block);
        final Expression lateral = Expressions.new_(
                LookupLateralEnumerable.class,
                leftEnumerable,
                Expressions.constant(sourceId, long.class),
                Expressions.constant(runtimeId, long.class),
                Expressions.constant(table),
                Expressions.constant(indexName, String.class),
                Expressions.constant(range),
                Expressions.constant(leftFieldCount),
                Expressions.constant(prefixLength),
                Expressions.constant(lowerOrdinal),
                Expressions.constant(upperOrdinal),
                Expressions.constant(lowerInclusive),
                Expressions.constant(upperInclusive),
                Expressions.constant(leafKeyPos),
                Expressions.constant(innerFieldCount),
                Expressions.constant(leftJoin));
        builder.add(Expressions.return_(null, lateral));
        return implementor.result(physType, builder.toBlock());
    }

    /** Converts the Hep-created {@link Convention#NONE} node to enumerable. */
    public static final ConverterRule CONVERTER_RULE = ConverterRule.Config.INSTANCE
            .withConversion(LookupLateralJoin.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "LookupLateralJoinConverterRule")
            .withRuleFactory(LateralConverterRule::new)
            .toRule(LateralConverterRule.class);

    private static final class LateralConverterRule extends ConverterRule {

        private LateralConverterRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            final LookupLateralJoin lateral = (LookupLateralJoin) rel;
            final RelNode input = convert(lateral.getInput(),
                    lateral.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE));
            return lateral.copy(
                    lateral.getTraitSet().replace(EnumerableConvention.INSTANCE), List.of(input));
        }
    }
}
