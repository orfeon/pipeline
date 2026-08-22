package com.mercari.solution.util.domain.attribution.tree;

import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.domain.attribution.LeafTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric-Tree Change Decomposition (Zhou et al., Algorithm 1): evaluates every node's two-period
 * value on a {@link LeafTable}, applies the Table 1 local change decompositions, and propagates
 * contributions from the root down ({@code C_c = contrib_c · C_v / Δy_v}) so that every MECE
 * child group sums exactly to its parent's contribution.
 *
 * <p>Not thread-safe (compiled expressions are cached per instance); create one per call site.
 */
public final class MetricTreeEngine {

    /** Relative tolerance for the additivity (residual) check of a local decomposition. */
    private static final double RESIDUAL_TOLERANCE = 1e-6;

    public enum Effect { root, delta, volume, rate, share }

    public record NodeResult(
            String measure,
            String node,
            String parent,
            String path,
            int depth,
            String dimension,
            String value,
            MetricTreeSpec.Decomposition decomposition,
            Effect effect,
            double baseline,
            double target,
            double delta,
            double localContribution,
            double contribution,
            double explanatoryPower,
            boolean degenerate,
            Double residual,
            boolean causalAdjusted,
            String estimator,
            CausalAdjustment.Diagnostics diagnostics,
            String warning,
            int rank,
            String measureExpression,
            Map<String, String> fixedDimensions,
            int[] leaves) implements Serializable {
    }

    public record TreeResult(String measure, double rootBaseline, double rootTarget, List<NodeResult> nodes) implements Serializable {

        public double rootDelta() {
            return rootTarget - rootBaseline;
        }
    }

    private final MetricTreeSpec spec;
    private final LeafTable table;
    private final Map<String, ExpressionUtil.Expression> expressions = new HashMap<>();
    private final int granuleDim;

    public MetricTreeEngine(final MetricTreeSpec spec, final LeafTable table) {
        this.spec = spec;
        this.table = table;
        final String granularity = spec.causal() == null ? null : spec.causal().granularityField();
        this.granuleDim = granularity == null ? -1 : table.dimensionIndex(granularity);
    }

    public TreeResult run(final String root) {
        final int[] all = new int[table.leafCount()];
        for(int i = 0; i < all.length; i++) {
            all[i] = i;
        }
        final MetricTreeSpec.Node rootNode = spec.node(root);
        final List<NodeResult> rows = new ArrayList<>();
        final double[] rootValue = value(rootNode, all);
        final double rootDelta = rootValue[1] - rootValue[0];
        propagate(root, rootNode, rootNode.breakdowns(), true, all,
                rootValue, rootDelta, rootDelta, null, root, 0, null, null, Effect.root, null, rows, null,
                expression(rootNode), Map.of());
        // Rank non-root nodes by |contribution|
        final List<NodeResult> ranked = new ArrayList<>();
        for(final NodeResult row : rows) {
            if(row.depth > 0) {
                ranked.add(row);
            }
        }
        ranked.sort(Comparator
                .comparingDouble((NodeResult r) -> -Math.abs(r.contribution))
                .thenComparingInt(r -> r.depth)
                .thenComparing(r -> r.path));
        final Map<String, Integer> rankByPath = new HashMap<>();
        int rank = 1;
        for(final NodeResult row : ranked) {
            rankByPath.put(row.path, rank++);
        }
        final List<NodeResult> out = new ArrayList<>(rows.size());
        for(final NodeResult row : rows) {
            out.add(withRank(row, row.depth == 0 ? 0 : rankByPath.get(row.path)));
        }
        return new TreeResult(root, rootValue[0], rootValue[1], out);
    }

    // ---- recursion ----

    /**
     * Emits the row for {@code node} evaluated on {@code leaves} and recurses into its children.
     *
     * @param withStatic whether the node's static children (components / volume+rate) apply
     *                   (false for breakdown-generated children, which only recurse into nested breakdowns)
     */
    private void propagate(
            final String measure,
            final MetricTreeSpec.Node node,
            final List<MetricTreeSpec.Breakdown> breakdowns,
            final boolean withStatic,
            final int[] leaves,
            final double[] value,
            final double localContribution,
            final double contribution,
            final String parentPath,
            final String path,
            final int depth,
            final String dimension,
            final String dimensionValue,
            final Effect effect,
            final MetricTreeSpec.Decomposition fromDecomposition,
            final List<NodeResult> rows,
            final CausalAdjustment.Result adjustment,
            final String measureExpression,
            final Map<String, String> fixedDimensions) {

        final double delta = value[1] - value[0];
        final double rootDelta = rows.isEmpty() ? delta : rows.getFirst().delta;
        final double ep = rootDelta == 0 ? 0 : contribution / rootDelta;
        final int rowIndex = rows.size();
        rows.add(new NodeResult(measure, node.name(), parentPath, path, depth, dimension, dimensionValue,
                fromDecomposition, effect, value[0], value[1], delta, localContribution, contribution, ep,
                false, null, adjustment != null && !"fallback".equals(adjustment.estimator()),
                adjustment == null ? null : adjustment.estimator(),
                adjustment == null ? null : adjustment.diagnostics(),
                adjustment == null ? null : adjustment.warning(), 0,
                measureExpression, fixedDimensions, leaves));

        boolean degenerate = false;
        Double residual = null;

        // ---- static children ----
        if(withStatic && node.decomposition() != null) {
            final List<Child> children = new ArrayList<>();
            switch (node.decomposition()) {
                case sum -> {
                    for(final String component : node.components()) {
                        final MetricTreeSpec.Node child = spec.node(component);
                        final double[] v = value(child, leaves);
                        children.add(new Child(child, child.breakdowns(), true, v, v[1] - v[0],
                                null, null, Effect.delta, null));
                    }
                }
                case product -> {
                    final MetricTreeSpec.Node volume = spec.node(node.volume());
                    final MetricTreeSpec.Node rate = spec.node(node.rate());
                    final double[] n = value(volume, leaves);
                    final double[] x = value(rate, leaves);
                    final MetricTreeSpec.Edge edge = spec.edgeTo(node.volume());
                    if(edge != null && edge.from().equals(node.rate())) {
                        final CausalAdjustment.Result result = adjust(edge, volume, rate, leaves, n, x, delta);
                        children.add(new Child(volume, volume.breakdowns(), true, n, result.volumeContribution(),
                                null, null, Effect.volume, result));
                        children.add(new Child(rate, rate.breakdowns(), true, x, result.rateContribution(),
                                null, null, Effect.rate, null));
                    } else {
                        // Ordered allocation (Fig. 1): rate changes first, volume follows
                        children.add(new Child(volume, volume.breakdowns(), true, n, (n[1] - n[0]) * x[1],
                                null, null, Effect.volume, null));
                        children.add(new Child(rate, rate.breakdowns(), true, x, (x[1] - x[0]) * n[0],
                                null, null, Effect.rate, null));
                    }
                }
                default -> throw new IllegalStateException("unexpected static decomposition " + node.decomposition());
            }
            final Group group = scale(children, delta, contribution);
            degenerate |= group.degenerate;
            residual = maxAbs(residual, group.residual);
            for(final Child child : children) {
                propagate(measure, child.node, child.breakdowns, child.withStatic, leaves, child.value,
                        child.localContribution, child.contribution(group), path,
                        path + "/" + child.node.name(), depth + 1, null, null, child.effect,
                        node.decomposition(), rows, child.adjustment, expression(child.node), fixedDimensions);
            }
        }

        // ---- breakdowns (each an independent MECE partition) ----
        if(breakdowns != null) {
            for(final MetricTreeSpec.Breakdown breakdown : breakdowns) {
                final int dim = table.dimensionIndex(breakdown.by());
                final Map<String, int[]> groups = groupLeaves(leaves, dim);
                final MetricTreeSpec.Decomposition type = breakdown.decomposition() == null
                        ? MetricTreeSpec.Decomposition.sum : breakdown.decomposition();
                final List<Child> children = new ArrayList<>();
                switch (type) {
                    case sum -> {
                        for(final Map.Entry<String, int[]> g : groups.entrySet()) {
                            final double[] v = value(node, g.getValue());
                            children.add(new Child(node, breakdown.breakdowns(), false, v, v[1] - v[0],
                                    breakdown.by(), g.getKey(), Effect.delta, null).withLeaves(g.getValue())
                                    .withExpression(measureExpression));
                        }
                    }
                    case sumOfProducts -> {
                        final int yCol = table.columnIndex(node.field());
                        final int nCol = table.columnIndex(breakdown.volume());
                        final MetricTreeSpec.Node volumeNode = new MetricTreeSpec.Node(
                                breakdown.volume(), breakdown.volume(), null, null, null, null, null, null);
                        final MetricTreeSpec.Node rateNode = new MetricTreeSpec.Node(
                                node.name() + "_rate", null, node.field() + " / " + breakdown.volume(),
                                null, null, null, null, null);
                        for(final Map.Entry<String, int[]> g : groups.entrySet()) {
                            final double[] y = sums(yCol, g.getValue());
                            final double[] n = sums(nCol, g.getValue());
                            final double[] x = { ratio(y[0], n[0]), ratio(y[1], n[1]) };
                            children.add(new Child(volumeNode, null, false, n, (n[1] - n[0]) * x[1],
                                    breakdown.by(), g.getKey(), Effect.volume, null).withLeaves(g.getValue())
                                    .withExpression(breakdown.volume()));
                            children.add(new Child(rateNode, breakdown.breakdowns(), false, x, (x[1] - x[0]) * n[0],
                                    breakdown.by(), g.getKey(), Effect.rate, null).withLeaves(g.getValue())
                                    .withExpression(rateNode.expression()));
                        }
                    }
                    case weightedAverage -> {
                        final int wCol = table.columnIndex(breakdown.weight());
                        final double[] wTotal = sums(wCol, leaves);
                        final MetricTreeSpec.Node shareNode = new MetricTreeSpec.Node(
                                node.name() + "_share", null, null, null, null, null, null, null);
                        for(final Map.Entry<String, int[]> g : groups.entrySet()) {
                            final double[] w = sums(wCol, g.getValue());
                            final double[] p = { ratio(w[0], wTotal[0]), ratio(w[1], wTotal[1]) };
                            final double[] r = value(node, g.getValue());
                            // share: the group's weight is what a drilldown can localize
                            children.add(new Child(shareNode, null, false, p, (p[1] - p[0]) * (r[0] - value[0]),
                                    breakdown.by(), g.getKey(), Effect.share, null).withLeaves(g.getValue())
                                    .withExpression(breakdown.weight()));
                            children.add(new Child(node, breakdown.breakdowns(), false, r, (r[1] - r[0]) * p[1],
                                    breakdown.by(), g.getKey(), Effect.rate, null).withLeaves(g.getValue())
                                    .withExpression(measureExpression));
                        }
                    }
                    default -> throw new IllegalStateException("unexpected breakdown decomposition " + type);
                }
                final Group group = scale(children, delta, contribution);
                degenerate |= group.degenerate;
                residual = maxAbs(residual, group.residual);
                for(final Child child : children) {
                    final String childPath = path + "/" + breakdown.by() + "=" + child.dimensionValue
                            + (Effect.delta.equals(child.effect) ? "" : "/" + child.effect.name());
                    final Map<String, String> fixed = new LinkedHashMap<>(fixedDimensions);
                    fixed.put(breakdown.by(), child.dimensionValue);
                    propagate(measure, child.node, child.breakdowns, false, child.leaves, child.value,
                            child.localContribution, child.contribution(group), path, childPath, depth + 1,
                            child.dimension, child.dimensionValue, child.effect, type, rows, null,
                            child.expression, fixed);
                }
            }
        }

        if(degenerate || residual != null) {
            final NodeResult row = rows.get(rowIndex);
            rows.set(rowIndex, new NodeResult(row.measure, row.node, row.parent, row.path, row.depth,
                    row.dimension, row.value, row.decomposition, row.effect, row.baseline, row.target, row.delta,
                    row.localContribution, row.contribution, row.explanatoryPower, degenerate, residual,
                    row.causalAdjusted, row.estimator, row.diagnostics, row.warning, row.rank,
                    row.measureExpression, row.fixedDimensions, row.leaves));
        }
    }

    private static Double maxAbs(final Double a, final Double b) {
        if(a == null) {
            return b;
        }
        if(b == null) {
            return a;
        }
        return Math.abs(a) >= Math.abs(b) ? a : b;
    }

    private static final class Child {
        final MetricTreeSpec.Node node;
        final List<MetricTreeSpec.Breakdown> breakdowns;
        final boolean withStatic;
        final double[] value;
        final double localContribution;
        final String dimension;
        final String dimensionValue;
        final Effect effect;
        final CausalAdjustment.Result adjustment;
        int[] leaves;
        String expression;

        Child(final MetricTreeSpec.Node node, final List<MetricTreeSpec.Breakdown> breakdowns, final boolean withStatic,
              final double[] value, final double localContribution, final String dimension, final String dimensionValue,
              final Effect effect, final CausalAdjustment.Result adjustment) {
            this.node = node;
            this.breakdowns = breakdowns;
            this.withStatic = withStatic;
            this.value = value;
            this.localContribution = Double.isFinite(localContribution) ? localContribution : 0.0;
            this.dimension = dimension;
            this.dimensionValue = dimensionValue;
            this.effect = effect;
            this.adjustment = adjustment;
        }

        Child withLeaves(final int[] leaves) {
            this.leaves = leaves;
            return this;
        }

        Child withExpression(final String expression) {
            this.expression = expression;
            return this;
        }

        double contribution(final Group group) {
            return group.degenerate ? 0.0 : localContribution * group.scale;
        }
    }

    private record Group(boolean degenerate, double scale, Double residual) {
    }

    /** Algorithm 1 lines 9-13 with the near-zero parent guard and the additivity check. */
    private Group scale(final List<Child> children, final double parentDelta, final double parentContribution) {
        double sum = 0;
        double sumAbs = 0;
        for(final Child child : children) {
            sum += child.localContribution;
            sumAbs += Math.abs(child.localContribution);
        }
        final double residualValue = parentDelta - sum;
        final Double residual = Math.abs(residualValue) > RESIDUAL_TOLERANCE * Math.max(1.0, Math.max(Math.abs(parentDelta), sumAbs))
                ? residualValue : null;
        final boolean degenerate = parentDelta == 0
                || Math.abs(parentDelta) < spec.minParentDeltaRatio() * sumAbs;
        return new Group(degenerate, degenerate ? 0.0 : parentContribution / parentDelta, residual);
    }

    // ---- node expressions (for drilldown measures) ----

    /**
     * The node's value as a Lucene expression over input columns: its own field/expression, or
     * the composition of its static children ({@code sum} → {@code (a)+(b)}, {@code product} →
     * {@code (n)*(x)}). Null when the node has no value definition.
     */
    public String expression(final MetricTreeSpec.Node node) {
        if(node.field() != null) {
            return node.field();
        }
        if(node.expression() != null) {
            return node.expression();
        }
        if(node.decomposition() == null) {
            return null;
        }
        return switch (node.decomposition()) {
            case sum -> {
                final List<String> parts = new ArrayList<>();
                for(final String component : node.components()) {
                    parts.add("(" + expression(spec.node(component)) + ")");
                }
                yield String.join(" + ", parts);
            }
            case product -> "(" + expression(spec.node(node.volume())) + ") * (" + expression(spec.node(node.rate())) + ")";
            default -> null;
        };
    }

    // ---- node values ----

    /** {baseline, target} value of the node on the given leaves. */
    double[] value(final MetricTreeSpec.Node node, final int[] leaves) {
        if(node.field() != null) {
            return sums(table.columnIndex(node.field()), leaves);
        }
        if(node.expression() != null) {
            final ExpressionUtil.Expression expression = expressions
                    .computeIfAbsent(node.expression(), ExpressionUtil::createDefaultExpression);
            final Map<String, Double> baseline = new HashMap<>();
            final Map<String, Double> target = new HashMap<>();
            for(final String variable : expression.getVariableNames()) {
                final double[] s = sums(table.columnIndex(variable), leaves);
                baseline.put(variable, s[0]);
                target.put(variable, s[1]);
            }
            return new double[]{ finite(expression.evaluate(baseline)), finite(expression.evaluate(target)) };
        }
        return switch (node.decomposition()) {
            case sum -> {
                final double[] total = new double[2];
                for(final String component : node.components()) {
                    final double[] v = value(spec.node(component), leaves);
                    total[0] += v[0];
                    total[1] += v[1];
                }
                yield total;
            }
            case product -> {
                final double[] n = value(spec.node(node.volume()), leaves);
                final double[] x = value(spec.node(node.rate()), leaves);
                yield new double[]{ n[0] * x[0], n[1] * x[1] };
            }
            default -> throw new IllegalStateException("node " + node.name() + " has no value definition");
        };
    }

    private double[] sums(final int column, final int[] leaves) {
        double b = 0;
        double t = 0;
        for(final int leaf : leaves) {
            b += table.baselineValue(column, leaf);
            t += table.targetValue(column, leaf);
        }
        return new double[]{ b, t };
    }

    private static double ratio(final double num, final double den) {
        return den == 0 ? 0.0 : num / den;
    }

    private static double finite(final double v) {
        return Double.isFinite(v) ? v : 0.0;
    }

    private Map<String, int[]> groupLeaves(final int[] leaves, final int dim) {
        final Map<String, List<Integer>> groups = new TreeMap<>();
        for(final int leaf : leaves) {
            groups.computeIfAbsent(table.dimValue(leaf, dim), k -> new ArrayList<>()).add(leaf);
        }
        final Map<String, int[]> out = new LinkedHashMap<>();
        for(final Map.Entry<String, List<Integer>> e : groups.entrySet()) {
            out.put(e.getKey(), e.getValue().stream().mapToInt(Integer::intValue).toArray());
        }
        return out;
    }

    // ---- causal adjustment ----

    private CausalAdjustment.Result adjust(
            final MetricTreeSpec.Edge edge,
            final MetricTreeSpec.Node volume,
            final MetricTreeSpec.Node rate,
            final int[] leaves,
            final double[] n,
            final double[] x,
            final double parentDelta) {

        final List<Double> x0 = new ArrayList<>();
        final List<Double> y0 = new ArrayList<>();
        final List<Double> x1 = new ArrayList<>();
        final List<Double> y1 = new ArrayList<>();
        if(granuleDim >= 0) {
            for(final int[] granule : groupLeaves(leaves, granuleDim).values()) {
                long baselineRows = 0;
                long targetRows = 0;
                for(final int leaf : granule) {
                    baselineRows += table.baselineRows(leaf);
                    targetRows += table.targetRows(leaf);
                }
                final double[] gn = value(volume, granule);
                final double[] gx = value(rate, granule);
                if(baselineRows > 0) {
                    x0.add(gx[0]);
                    y0.add(gn[0]);
                }
                if(targetRows > 0) {
                    x1.add(gx[1]);
                    y1.add(gn[1]);
                }
            }
        }
        return CausalAdjustment.adjust(edge, spec.causal(),
                toArray(x0), toArray(y0), toArray(x1), toArray(y1),
                n[0], n[1], x[0], x[1], parentDelta);
    }

    private static double[] toArray(final List<Double> values) {
        final double[] array = new double[values.size()];
        for(int i = 0; i < array.length; i++) {
            array[i] = values.get(i);
        }
        return array;
    }

    private static NodeResult withRank(final NodeResult r, final int rank) {
        return new NodeResult(r.measure, r.node, r.parent, r.path, r.depth, r.dimension, r.value, r.decomposition,
                r.effect, r.baseline, r.target, r.delta, r.localContribution, r.contribution, r.explanatoryPower,
                r.degenerate, r.residual, r.causalAdjusted, r.estimator, r.diagnostics, r.warning, rank,
                r.measureExpression, r.fixedDimensions, r.leaves);
    }
}
