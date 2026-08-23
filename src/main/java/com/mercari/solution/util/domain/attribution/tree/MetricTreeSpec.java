package com.mercari.solution.util.domain.attribution.tree;

import com.mercari.solution.util.ExpressionUtil;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Declarative metric tree for {@code vocabulary.unit: metric} — the Metric-Tree Change
 * Decomposition (MTCD) framework of Zhou et al., "A Unified Approach to Interpretable Causal
 * Root Cause Attribution" (KDD '26 TSMO workshop), Table 1 and Appendix A.
 *
 * <p>A node's two-period value is obtained from a {@code field} (sum of an additive input column),
 * an {@code expression} (Lucene expression over period-level field sums, e.g. a ratio of sums), or
 * implicitly from its static children. Children come in two flavors:
 *
 * <ul>
 *   <li><b>static</b> — other declared nodes: {@code components} (Type 1 sum) or
 *       {@code volume}/{@code rate} (Type 2 product);</li>
 *   <li><b>breakdowns</b> — dynamic children per value of a dimension column
 *       ({@code sum} Type 1, {@code sumOfProducts} Type 3, {@code weightedAverage} Type 4). Each
 *       breakdown is an independent MECE partition of the node's change (Algorithm 1's
 *       {@code dim(c)}); nested breakdowns recurse on the node restricted to the group.</li>
 * </ul>
 *
 * <p>Causal edges (Sec. 6) are restricted to siblings of a {@code product} node: the rate child
 * drives the volume child ({@code from: rate, to: volume}); the volume contribution becomes the
 * unbiased estimator of Theorem 6.1 / Proposition 6.2 and the rate child receives the remainder
 * (direct + indirect effect) — ordered allocation along the causal path.
 */
public record MetricTreeSpec(
        List<Node> nodes,
        List<Edge> edges,
        Causal causal,
        double minParentDeltaRatio) implements Serializable {

    public static final double DEFAULT_MIN_PARENT_DELTA_RATIO = 0.01;

    public enum Decomposition {
        sum,              // Type 1
        product,          // Type 2 (static volume/rate children only)
        sumOfProducts,    // Type 3 (breakdown only)
        weightedAverage   // Type 4 (breakdown only)
    }

    public enum Model { linear, quadratic }

    public enum Estimator { auto, simplified, full }

    public record Node(
            String name,
            String field,
            String expression,
            Decomposition decomposition,
            List<String> components,
            String volume,
            String rate,
            List<Breakdown> breakdowns) implements Serializable {

        public boolean hasStaticChildren() {
            return decomposition != null;
        }

        /** Input columns this node's own value depends on (fields, expression variables). */
        public Set<String> ownFields() {
            final Set<String> fields = new TreeSet<>();
            if(field != null) {
                fields.add(field);
            }
            if(expression != null) {
                fields.addAll(ExpressionUtil.estimateVariables(expression));
            }
            return fields;
        }

        public List<String> staticChildren() {
            if(decomposition == null) {
                return List.of();
            }
            return switch (decomposition) {
                case sum -> components == null ? List.of() : components;
                case product -> List.of(volume, rate);
                default -> List.of();
            };
        }
    }

    public record Breakdown(
            String by,
            Decomposition decomposition,
            String volume,   // sumOfProducts: additive field n_k
            String weight,   // weightedAverage: additive field for p_k
            List<Breakdown> breakdowns) implements Serializable {

        public Set<String> ownFields() {
            final Set<String> fields = new TreeSet<>();
            if(volume != null) {
                fields.add(volume);
            }
            if(weight != null) {
                fields.add(weight);
            }
            if(breakdowns != null) {
                for(final Breakdown nested : breakdowns) {
                    fields.addAll(nested.ownFields());
                }
            }
            return fields;
        }
    }

    public record Edge(
            String from,
            String to,
            Model model,
            boolean robust,
            Estimator estimator,
            Double elasticity) implements Serializable {
    }

    public record Causal(
            String granularityField,
            double slopeStabilityAlpha,
            int minGranules) implements Serializable {

        public static final double DEFAULT_ALPHA = 0.05;
        public static final int DEFAULT_MIN_GRANULES = 14;
    }

    public Node node(final String name) {
        for(final Node node : nodes) {
            if(node.name.equals(name)) {
                return node;
            }
        }
        return null;
    }

    /** Edge whose {@code to} is the given node, or null. */
    public Edge edgeTo(final String to) {
        if(edges == null) {
            return null;
        }
        for(final Edge edge : edges) {
            if(edge.to.equals(to)) {
                return edge;
            }
        }
        return null;
    }

    /** Every input column referenced anywhere in the tree. */
    public Set<String> referencedFields() {
        final Set<String> fields = new LinkedHashSet<>();
        for(final Node node : nodes) {
            fields.addAll(node.ownFields());
            if(node.breakdowns != null) {
                for(final Breakdown breakdown : node.breakdowns) {
                    fields.addAll(breakdown.ownFields());
                }
            }
        }
        return fields;
    }

    /** Every dimension column used by a breakdown anywhere in the tree. */
    public Set<String> breakdownDimensions() {
        final Set<String> dimensions = new LinkedHashSet<>();
        for(final Node node : nodes) {
            collectDimensions(node.breakdowns, dimensions);
        }
        return dimensions;
    }

    private static void collectDimensions(final List<Breakdown> breakdowns, final Set<String> out) {
        if(breakdowns == null) {
            return;
        }
        for(final Breakdown breakdown : breakdowns) {
            out.add(breakdown.by);
            collectDimensions(breakdown.breakdowns, out);
        }
    }

    /**
     * Structural validation, independent of input schemas. Returns human-readable messages
     * (empty when valid). Messages are prefixed with {@code location} by the caller.
     */
    public List<String> validate(final Set<String> roots) {
        final List<String> errors = new ArrayList<>();
        if(nodes == null || nodes.isEmpty()) {
            errors.add("tree.nodes must not be empty");
            return errors;
        }
        final Map<String, Node> byName = new HashMap<>();
        for(final Node node : nodes) {
            if(node.name == null || node.name.isEmpty()) {
                errors.add("tree.nodes[].name is required");
                continue;
            }
            if(byName.put(node.name, node) != null) {
                errors.add("tree.nodes[" + node.name + "] is declared more than once");
            }
        }
        for(final String root : roots) {
            if(!byName.containsKey(root)) {
                errors.add("measures[" + root + "] must name a tree node when vocabulary.unit is metric");
            }
        }
        for(final Node node : nodes) {
            if(node.name == null) {
                continue;
            }
            final String loc = "tree.nodes[" + node.name + "]";
            if(node.field != null && node.expression != null) {
                errors.add(loc + " must not set both field and expression");
            }
            if(node.expression != null) {
                try {
                    ExpressionUtil.createDefaultExpression(node.expression);
                    if(ExpressionUtil.estimateVariables(node.expression).isEmpty()) {
                        errors.add(loc + ".expression must contain at least one variable");
                    }
                } catch (final Throwable e) {
                    errors.add(loc + ".expression is invalid: " + e.getMessage());
                }
            }
            if(node.decomposition != null) {
                switch (node.decomposition) {
                    case sum -> {
                        if(node.components == null || node.components.isEmpty()) {
                            errors.add(loc + ".components is required for decomposition: sum");
                        } else {
                            for(final String component : node.components) {
                                if(!byName.containsKey(component)) {
                                    errors.add(loc + ".components references unknown node: " + component);
                                }
                            }
                        }
                        if(node.volume != null || node.rate != null) {
                            errors.add(loc + ".volume/rate must not be set for decomposition: sum");
                        }
                    }
                    case product -> {
                        if(node.volume == null || node.rate == null) {
                            errors.add(loc + ".volume and .rate are required for decomposition: product");
                        } else {
                            if(!byName.containsKey(node.volume)) {
                                errors.add(loc + ".volume references unknown node: " + node.volume);
                            }
                            if(!byName.containsKey(node.rate)) {
                                errors.add(loc + ".rate references unknown node: " + node.rate);
                            }
                            if(node.volume.equals(node.rate)) {
                                errors.add(loc + ".volume and .rate must be different nodes");
                            }
                        }
                        if(node.components != null) {
                            errors.add(loc + ".components must not be set for decomposition: product");
                        }
                    }
                    default -> errors.add(loc + ".decomposition: " + node.decomposition
                            + " is only valid inside breakdowns (use sum or product for static children)");
                }
            } else {
                if(node.components != null || node.volume != null || node.rate != null) {
                    errors.add(loc + " has components/volume/rate but no decomposition");
                }
                if(node.field == null && node.expression == null) {
                    errors.add(loc + " needs a field, an expression, or a decomposition with static children");
                }
            }
            validateBreakdowns(loc, node, node.breakdowns, errors, new HashSet<>());
        }
        // Static children must form a DAG (a node may be shared by several parents)
        detectCycles(byName, errors);
        // Causal edges: siblings of a product node, rate -> volume, at most one per target
        if(edges != null && !edges.isEmpty()) {
            final Set<String> targets = new HashSet<>();
            for(final Edge edge : edges) {
                final String loc = "causal.edges[" + edge.from + "->" + edge.to + "]";
                if(edge.from == null || edge.to == null) {
                    errors.add("causal.edges[].from and .to are required");
                    continue;
                }
                if(!targets.add(edge.to)) {
                    errors.add(loc + ": node " + edge.to + " is the target of more than one edge");
                }
                Node parent = null;
                for(final Node node : nodes) {
                    if(Decomposition.product.equals(node.decomposition)
                            && edge.from.equals(node.rate) && edge.to.equals(node.volume)) {
                        parent = node;
                        break;
                    }
                }
                if(parent == null) {
                    errors.add(loc + ": causal edges are only supported between the rate (from) and"
                            + " volume (to) children of a product node");
                }
                if(edge.elasticity == null && (causal == null || causal.granularityField == null)) {
                    errors.add(loc + ": causal.granularity.field is required to estimate the edge"
                            + " (or set elasticity for a known slope)");
                }
            }
        }
        if(causal != null) {
            if(causal.slopeStabilityAlpha <= 0 || causal.slopeStabilityAlpha >= 1) {
                errors.add("causal.slopeStabilityAlpha must be in (0, 1)");
            }
            if(causal.minGranules < 3) {
                errors.add("causal.minGranules must be at least 3");
            }
        }
        if(minParentDeltaRatio < 0 || minParentDeltaRatio >= 1) {
            errors.add("engine.metricTree.minParentDeltaRatio must be in [0, 1)");
        }
        return errors;
    }

    private static void validateBreakdowns(
            final String loc, final Node node, final List<Breakdown> breakdowns,
            final List<String> errors, final Set<String> enclosing) {

        if(breakdowns == null) {
            return;
        }
        for(final Breakdown breakdown : breakdowns) {
            if(breakdown.by == null || breakdown.by.isEmpty()) {
                errors.add(loc + ".breakdowns[].by is required");
                continue;
            }
            final String bloc = loc + ".breakdowns[" + breakdown.by + "]";
            if(enclosing.contains(breakdown.by)) {
                errors.add(bloc + " is nested inside a breakdown of the same dimension");
            }
            final Decomposition type = breakdown.decomposition == null ? Decomposition.sum : breakdown.decomposition;
            switch (type) {
                case sum -> {
                    if(node.field == null && !Decomposition.sum.equals(node.decomposition)) {
                        errors.add(bloc + " decomposition: sum requires an additive node (field or sum of components)");
                    }
                    if(breakdown.volume != null || breakdown.weight != null) {
                        errors.add(bloc + ".volume/weight must not be set for decomposition: sum");
                    }
                }
                case sumOfProducts -> {
                    if(node.field == null) {
                        errors.add(bloc + " decomposition: sumOfProducts requires the node to be an additive field");
                    }
                    if(breakdown.volume == null) {
                        errors.add(bloc + ".volume (additive field n_k) is required for decomposition: sumOfProducts");
                    }
                    if(breakdown.weight != null) {
                        errors.add(bloc + ".weight must not be set for decomposition: sumOfProducts");
                    }
                }
                case weightedAverage -> {
                    if(node.expression == null) {
                        errors.add(bloc + " decomposition: weightedAverage requires a rate node (expression)");
                    }
                    if(breakdown.weight == null) {
                        errors.add(bloc + ".weight (additive field for the share p_k) is required for decomposition: weightedAverage");
                    }
                    if(breakdown.volume != null) {
                        errors.add(bloc + ".volume must not be set for decomposition: weightedAverage");
                    }
                }
                case product -> errors.add(bloc + " decomposition: product is not a breakdown type");
            }
            final Set<String> nested = new HashSet<>(enclosing);
            nested.add(breakdown.by);
            validateBreakdowns(bloc, node, breakdown.breakdowns, errors, nested);
        }
    }

    private static void detectCycles(final Map<String, Node> byName, final List<String> errors) {
        final Map<String, Integer> state = new HashMap<>(); // 1 = visiting, 2 = done
        for(final Node node : byName.values()) {
            if(visit(node.name, byName, state)) {
                errors.add("tree.nodes: static children form a cycle through node " + node.name);
                return;
            }
        }
    }

    private static boolean visit(final String name, final Map<String, Node> byName, final Map<String, Integer> state) {
        final Integer s = state.get(name);
        if(s != null) {
            return s == 1;
        }
        state.put(name, 1);
        final Node node = byName.get(name);
        if(node != null) {
            for(final String child : node.staticChildren()) {
                if(child != null && byName.containsKey(child) && visit(child, byName, state)) {
                    return true;
                }
            }
        }
        state.put(name, 2);
        return false;
    }

    /** Nodes reachable from the root through static children, root first (BFS). */
    public List<String> reachable(final String root) {
        final List<String> order = new ArrayList<>();
        final Set<String> seen = new HashSet<>();
        final Deque<String> queue = new ArrayDeque<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            final String name = queue.poll();
            if(!seen.add(name)) {
                continue;
            }
            order.add(name);
            final Node node = node(name);
            if(node != null) {
                queue.addAll(node.staticChildren());
            }
        }
        return order;
    }
}
