package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class DerivedAllocationTest {

    private static final MeasureSpec CVR = MeasureSpec.derived(
            "cvr", "orders / sessions", List.of("orders", "sessions"));

    /**
     * Two leaves: leaf A doubles its orders contribution (10 -> 30), leaf B is unchanged.
     * Global cvr: 20/200 = 0.1 -> 40/200 = 0.2.
     */
    private static LeafTable fixture() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d"), List.of("orders", "sessions"));
        builder.addBaseline(new String[]{"A"}, new double[]{10, 100});
        builder.addTarget(new String[]{"A"}, new double[]{30, 100});
        builder.addBaseline(new String[]{"B"}, new double[]{10, 100});
        builder.addTarget(new String[]{"B"}, new double[]{10, 100});
        return builder.build();
    }

    @Test
    public void testGre() {
        final MeasureVector vector = DerivedAllocation.allocate(
                DerivedAllocation.Method.gre, fixture(), CVR);

        // Leaf-level evaluation for deviation scoring
        Assertions.assertArrayEquals(new double[]{0.1, 0.1}, vector.baseline(), 1e-9);
        Assertions.assertArrayEquals(new double[]{0.3, 0.1}, vector.target(), 1e-9);
        // Substitution effect: leaf A h(40,200)-h(20,200) = 0.1, leaf B 0 -> normalized [1, 0]
        Assertions.assertArrayEquals(new double[]{1.0, 0.0}, vector.explanatoryPowers(), 1e-9);
    }

    @Test
    public void testGreMatchesReferenceRatioFormula() {
        // Reference (element_scores.add_explanatory_power, derived=True):
        // ep = ((Δa)·F_b - (Δb)·F_a) / (F_b·(F_b + Δb)), normalized
        final LeafTable table = fixture();
        final MeasureVector vector = DerivedAllocation.allocate(
                DerivedAllocation.Method.gre, table, CVR);

        final double fa = 20, fb = 200;
        final double epA = ((30 - 10) * fb - 0 * fa) / (fb * (fb + 0));
        final double epB = 0;
        final double norm = epA + epB;
        Assertions.assertEquals(epA / norm, vector.explanatoryPowers()[0], 1e-9);
        Assertions.assertEquals(epB, vector.explanatoryPowers()[1], 1e-9);
    }

    @Test
    public void testPartialDerivative() {
        final MeasureVector vector = DerivedAllocation.allocate(
                DerivedAllocation.Method.partialDerivative, fixture(), CVR);

        // Linearized pseudo delta sums to the derived delta (exact here: h is linear in orders)
        final double deltaSum = vector.targetTotal() - vector.baselineTotal();
        Assertions.assertEquals(0.1, deltaSum, 1e-6);
        // All of the delta is attributed to leaf A
        Assertions.assertEquals(0.1, vector.target()[0] - vector.baseline()[0], 1e-6);
        Assertions.assertEquals(0.0, vector.target()[1] - vector.baseline()[1], 1e-6);
    }

    @Test
    public void testShapleyEfficiencyAndTwoPlayerClosedForm() {
        final MeasureVector vector = DerivedAllocation.allocate(
                DerivedAllocation.Method.shapley, fixture(), CVR);

        // Efficiency axiom: pseudo delta sums to h(V) - h(F) = 0.1
        Assertions.assertEquals(0.1, vector.targetTotal() - vector.baselineTotal(), 1e-9);
        // K=2 closed form: φ_orders = 0.5[(v({o})-v({})) + (v({o,s})-v({s}))] = 0.1, φ_sessions = 0.
        // Orders delta is entirely on leaf A.
        Assertions.assertEquals(0.1, vector.target()[0] - vector.baseline()[0], 1e-9);
        Assertions.assertEquals(0.0, vector.target()[1] - vector.baseline()[1], 1e-9);
    }

    @Test
    public void testShapleyVariableLimit() {
        final int k = DerivedAllocation.MAX_SHAPLEY_VARIABLES + 1;
        final StringBuilder expression = new StringBuilder();
        final List<String> variables = new java.util.ArrayList<>();
        final LeafTable.Builder builder;
        final String[] names = new String[k];
        for(int i = 0; i < k; i++) {
            names[i] = "x" + i;
            variables.add(names[i]);
            if(i > 0) {
                expression.append(" + ");
            }
            expression.append(names[i]);
        }
        builder = LeafTable.builder(List.of("d"), variables);
        builder.addBaseline(new String[]{"A"}, new double[k]);
        builder.addTarget(new String[]{"A"}, new double[k]);

        final MeasureSpec measure = MeasureSpec.derived("m", expression.toString(), variables);
        Assertions.assertThrows(IllegalArgumentException.class, () -> DerivedAllocation.allocate(
                DerivedAllocation.Method.shapley, builder.build(), measure));
    }

    @Test
    public void testEvaluateGuardsDivisionByZero() {
        Assertions.assertEquals(0.0, DerivedAllocation.evaluate(
                CVR, Map.of("orders", 10.0, "sessions", 0.0)), 1e-9);
        Assertions.assertEquals(0.25, DerivedAllocation.evaluate(
                CVR, Map.of("orders", 10.0, "sessions", 40.0)), 1e-9);
    }
}
