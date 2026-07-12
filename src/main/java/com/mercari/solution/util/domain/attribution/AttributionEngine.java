package com.mercari.solution.util.domain.attribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point of the attribution core: conditions the leaf table (binning → optional synthetic
 * baseline → guards), resolves each measure to per-leaf vectors (derived measures via
 * {@link DerivedAllocation}), dispatches to the configured algorithm and assembles the result.
 * Pure computation — no engine dependencies; callers (Beam transform, future Calcite table
 * function / MCP tool) handle I/O and validation.
 */
public final class AttributionEngine {

    /**
     * {@code epBasis: auto} falls back from netDelta to absoluteDelta when the net change is
     * below this share of the total churn: {@code |V - F| < ratio * Σ|v - f|}. This covers both
     * the synthetic marginal baseline (net change is 0 by construction) and mix shifts
     * (large movements between slices that cancel out in the totals), where the netDelta
     * explanatory power is undefined or explodes.
     */
    public static final double AUTO_ABSOLUTE_FALLBACK_RATIO = 0.05;

    private AttributionEngine() {
    }

    public static AttributionResult run(
            final LeafTable raw,
            final List<DimensionSpec> dimensions,
            final List<MeasureSpec> measures,
            final EngineConfig config,
            final boolean syntheticMarginal) {

        LeafTable binned = Preprocess.bin(raw, dimensions);
        if(syntheticMarginal) {
            binned = SyntheticReference.marginal(binned);
        }
        final LeafTable table = Preprocess.applyGuards(binned, config.guards());

        final AttributionAlgorithm algorithm = AttributionAlgorithm.of(config.algorithm());

        final List<MeasureResult> results = new ArrayList<>();
        for(final MeasureSpec measure : measures) {
            final boolean derived = MeasureSpec.Type.derived.equals(measure.type());
            MeasureVector vector = derived
                    ? DerivedAllocation.allocate(config.derivedAllocation(), table, measure)
                    : table.measureVector(measure.name());

            final EngineConfig.EpBasis resolvedBasis = resolveEpBasis(vector, config.epBasis());
            if(EngineConfig.EpBasis.absoluteDelta.equals(resolvedBasis) && vector.explanatoryPower() == null) {
                vector = absoluteDeltaEp(vector);
            }

            List<Finding> findings = algorithm.localize(table, vector, config);

            final double baselineTotal;
            final double targetTotal;
            if(derived) {
                baselineTotal = evaluateComponents(table, measure, null, true);
                targetTotal = evaluateComponents(table, measure, null, false);
                // Pseudo-column sums are not interpretable for derived measures:
                // recompute slice values as h over the slice's component sums
                findings = findings.stream()
                        .map(finding -> new Finding(
                                finding.slices(),
                                finding.riskScore(),
                                finding.explanatoryPower(),
                                finding.surprise(),
                                evaluateComponents(table, measure, finding.slices(), true),
                                evaluateComponents(table, measure, finding.slices(), false),
                                finding.leafCount()))
                        .toList();
            } else {
                baselineTotal = vector.baselineTotal();
                targetTotal = vector.targetTotal();
            }
            results.add(new MeasureResult(measure.name(), baselineTotal, targetTotal, resolvedBasis, findings));
        }
        return new AttributionResult(results);
    }

    /**
     * Resolves the effective explanatory-power basis for a measure vector.
     * Vectors that carry their own allocation (gre derived measures) are reported as netDelta —
     * their explanatory power is a signed share of the net change by construction.
     */
    private static EngineConfig.EpBasis resolveEpBasis(final MeasureVector vector, final EngineConfig.EpBasis requested) {
        if(vector.explanatoryPower() != null) {
            return EngineConfig.EpBasis.netDelta;
        }
        return switch (requested == null ? EngineConfig.EpBasis.auto : requested) {
            case netDelta -> EngineConfig.EpBasis.netDelta;
            case absoluteDelta -> EngineConfig.EpBasis.absoluteDelta;
            case auto -> {
                double churn = 0;
                for(int i = 0; i < vector.size(); i++) {
                    churn += Math.abs(vector.target()[i] - vector.baseline()[i]);
                }
                final double netDelta = Math.abs(vector.targetTotal() - vector.baselineTotal());
                yield churn > 0 && netDelta < AUTO_ABSOLUTE_FALLBACK_RATIO * churn
                        ? EngineConfig.EpBasis.absoluteDelta
                        : EngineConfig.EpBasis.netDelta;
            }
        };
    }

    private static MeasureVector absoluteDeltaEp(final MeasureVector vector) {
        final double[] ep = new double[vector.size()];
        double sum = 0;
        for(int i = 0; i < ep.length; i++) {
            ep[i] = Math.abs(vector.target()[i] - vector.baseline()[i]);
            sum += ep[i];
        }
        if(sum > 0) {
            for(int i = 0; i < ep.length; i++) {
                ep[i] /= sum;
            }
        }
        return new MeasureVector(vector.baseline(), vector.target(), ep);
    }

    /**
     * Evaluates a derived measure over the component sums of the leaves covered by any of the
     * given slices ({@code null} slices = all leaves).
     */
    private static double evaluateComponents(
            final LeafTable table,
            final MeasureSpec measure,
            final List<Slice> slices,
            final boolean baseline) {

        final Map<String, Double> componentSums = new HashMap<>();
        for(final String variable : measure.variables()) {
            final int column = table.columnIndex(variable);
            double sum = 0;
            for(int leaf = 0; leaf < table.leafCount(); leaf++) {
                if(slices == null || covered(slices, table.dims(leaf))) {
                    sum += baseline ? table.baselineValue(column, leaf) : table.targetValue(column, leaf);
                }
            }
            componentSums.put(variable, sum);
        }
        return DerivedAllocation.evaluate(measure, componentSums);
    }

    private static boolean covered(final List<Slice> slices, final String[] dims) {
        for(final Slice slice : slices) {
            if(slice.contains(dims)) {
                return true;
            }
        }
        return false;
    }
}
