package com.mercari.solution.util.domain.attribution;

import org.apache.datasketches.kll.KllDoublesSketch;

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

    /** External judgment: minimum relative net change for a measure to count as "moved". */
    public static final double EXTERNAL_MIN_CHANGE_RATIO = 0.05;

    /**
     * External judgment for squeeze: findings whose minimum generalized potential score is below
     * this are considered weak explanations (the PSqueeze reference's fallback threshold; its
     * data-driven per-dataset calibration is not applicable to a single run).
     */
    public static final double EXTERNAL_SCORE_THRESHOLD = 0.8;

    /**
     * External judgment for the other algorithms: reports explaining less than 65% of the change
     * are considered weak (adapted from the PSqueeze reference's {@code ep < 0.65} criterion).
     */
    public static final double EXTERNAL_UNEXPLAINED_THRESHOLD = 0.35;

    private AttributionEngine() {
    }

    /**
     * Judges whether a measure's change is likely caused by an <b>external root cause</b> —
     * real, but not localizable in the declared dimensions. Adapted from PSqueeze's external
     * root cause determination (ADR-10): the measure must have moved (relative net change ≥
     * {@value #EXTERNAL_MIN_CHANGE_RATIO}); it is then external when no findings were reported,
     * when squeeze's weakest finding scores below {@value #EXTERNAL_SCORE_THRESHOLD}, or when
     * the report leaves more than {@value #EXTERNAL_UNEXPLAINED_THRESHOLD} of the change
     * unexplained (skipped when the findings were truncated at {@code topK}, where the
     * unexplained share is inflated by design).
     *
     * <p>Measures whose totals cancel by construction (synthetic marginal) never qualify as
     * "moved" and always return false.</p>
     */
    public static boolean externalRootCauseCandidate(final MeasureResult result, final EngineConfig config) {
        final double delta = Math.abs(result.targetTotal() - result.baselineTotal());
        final boolean moved = result.baselineTotal() == 0
                ? result.targetTotal() != 0
                : delta / Math.abs(result.baselineTotal()) >= EXTERNAL_MIN_CHANGE_RATIO;
        if(!moved) {
            return false;
        }
        if(result.findings().isEmpty()) {
            return true;
        }
        if(EngineConfig.Algorithm.squeeze.equals(config.algorithm())) {
            // Faithful to the reference: squeeze judges on its scores alone
            double minScore = Double.POSITIVE_INFINITY;
            for(final Finding finding : result.findings()) {
                if(finding.riskScore() != null) {
                    minScore = Math.min(minScore, finding.riskScore());
                }
            }
            return minScore < EXTERNAL_SCORE_THRESHOLD;
        }
        final boolean truncated = result.findings().size() >= config.topK();
        return !truncated && result.unexplainedShare() > EXTERNAL_UNEXPLAINED_THRESHOLD;
    }

    public static AttributionResult run(
            final LeafTable raw,
            final List<DimensionSpec> dimensions,
            final List<MeasureSpec> measures,
            final EngineConfig config,
            final boolean syntheticMarginal) {

        LeafTable binned = Preprocess.bin(raw, dimensions);
        if(syntheticMarginal) {
            if(binned.distributionCount() > 0 || binned.distinctCount() > 0) {
                throw new IllegalArgumentException(
                        "synthetic marginal baseline does not support distribution or distinct measures");
            }
            binned = SyntheticReference.marginal(binned);
        }
        final LeafTable table = Preprocess.applyGuards(binned, config.guards());

        final AttributionAlgorithm algorithm = AttributionAlgorithm.of(config.algorithm());

        final List<MeasureResult> results = new ArrayList<>();
        for(final MeasureSpec measure : measures) {
            if(MeasureSpec.Type.distribution.equals(measure.type())) {
                results.addAll(runDistribution(table, measure, algorithm, config));
                continue;
            }
            if(MeasureSpec.Type.distinct.equals(measure.type())) {
                results.add(runDistinct(table, measure, algorithm, config));
                continue;
            }
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
     * Localizes one distribution measure at each of its quantiles independently.
     * Per-leaf f/v are the leaf sketches' quantile values; the supplied explanatory power is the
     * mass-weighted absolute quantile shift share {@code (n_f + n_v)·|v - f|} normalized to sum 1
     * (an absoluteDelta-basis semantic — quantiles are not additive, so a netDelta share is not
     * defined). Finding and total values are quantiles of the merged sketches, never sums.
     */
    private static List<MeasureResult> runDistribution(
            final LeafTable table,
            final MeasureSpec measure,
            final AttributionAlgorithm algorithm,
            final EngineConfig config) {

        final int distribution = table.distributionIndex(measure.name());
        final List<MeasureResult> results = new ArrayList<>();
        for(final Double quantile : measure.quantiles()) {
            final MeasureVector vector = distributionVector(table, distribution, quantile);
            final List<Finding> findings = algorithm.localize(table, vector, config).stream()
                    .map(finding -> new Finding(
                            finding.slices(),
                            finding.riskScore(),
                            finding.explanatoryPower(),
                            finding.surprise(),
                            mergedQuantile(table, distribution, finding.slices(), quantile, true),
                            mergedQuantile(table, distribution, finding.slices(), quantile, false),
                            finding.leafCount()))
                    .toList();
            results.add(new MeasureResult(
                    measure.name(),
                    quantile,
                    mergedQuantile(table, distribution, null, quantile, true),
                    mergedQuantile(table, distribution, null, quantile, false),
                    EngineConfig.EpBasis.absoluteDelta,
                    findings));
        }
        return results;
    }

    /**
     * Localizes one distinct-count measure. Per-leaf f/v are the leaf Theta sketches' distinct
     * estimates; the supplied explanatory power is the absolute estimate shift share
     * {@code |v - f|} normalized to sum 1 (an absoluteDelta-basis semantic — identities can span
     * leaves, so leaf-level deltas do not decompose the union delta exactly and a net-change
     * share is not defined). Finding and total values are union estimates, never sums.
     */
    private static MeasureResult runDistinct(
            final LeafTable table,
            final MeasureSpec measure,
            final AttributionAlgorithm algorithm,
            final EngineConfig config) {

        final int distinct = table.distinctIndex(measure.name());
        final int leafCount = table.leafCount();
        final double[] f = new double[leafCount];
        final double[] v = new double[leafCount];
        final double[] ep = new double[leafCount];
        double weightSum = 0;
        for(int leaf = 0; leaf < leafCount; leaf++) {
            f[leaf] = estimateOf(table.baselineDistinct(distinct, leaf));
            v[leaf] = estimateOf(table.targetDistinct(distinct, leaf));
            ep[leaf] = Math.abs(v[leaf] - f[leaf]);
            weightSum += ep[leaf];
        }
        if(weightSum > 0) {
            for(int leaf = 0; leaf < leafCount; leaf++) {
                ep[leaf] /= weightSum;
            }
        }
        final List<Finding> findings = algorithm.localize(table, new MeasureVector(f, v, ep), config).stream()
                .map(finding -> new Finding(
                        finding.slices(),
                        finding.riskScore(),
                        finding.explanatoryPower(),
                        finding.surprise(),
                        unionEstimate(table, distinct, finding.slices(), true),
                        unionEstimate(table, distinct, finding.slices(), false),
                        finding.leafCount()))
                .toList();
        return new MeasureResult(
                measure.name(),
                unionEstimate(table, distinct, null, true),
                unionEstimate(table, distinct, null, false),
                EngineConfig.EpBasis.absoluteDelta,
                findings);
    }

    private static double estimateOf(final org.apache.datasketches.theta.Sketch sketch) {
        return sketch == null ? 0.0 : sketch.getEstimate();
    }

    /** Distinct estimate of the union of the given slices' leaf sketches ({@code null} slices = all leaves). */
    private static double unionEstimate(
            final LeafTable table,
            final int distinct,
            final List<Slice> slices,
            final boolean baseline) {

        final org.apache.datasketches.theta.Union union = org.apache.datasketches.theta.SetOperation
                .builder().setLogNominalEntries(LeafTable.THETA_LG_K).buildUnion();
        boolean any = false;
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            if(slices == null || covered(slices, table.dims(leaf))) {
                final org.apache.datasketches.theta.CompactSketch sketch = baseline
                        ? table.baselineDistinct(distinct, leaf)
                        : table.targetDistinct(distinct, leaf);
                if(sketch != null && !sketch.isEmpty()) {
                    union.union(sketch);
                    any = true;
                }
            }
        }
        return any ? union.getResult().getEstimate() : 0.0;
    }

    private static MeasureVector distributionVector(
            final LeafTable table, final int distribution, final double quantile) {

        final int leafCount = table.leafCount();
        final double[] f = new double[leafCount];
        final double[] v = new double[leafCount];
        final double[] ep = new double[leafCount];
        double weightSum = 0;
        for(int leaf = 0; leaf < leafCount; leaf++) {
            final KllDoublesSketch fs = table.baselineSketch(distribution, leaf);
            final KllDoublesSketch vs = table.targetSketch(distribution, leaf);
            f[leaf] = quantileOf(fs, quantile);
            v[leaf] = quantileOf(vs, quantile);
            final double mass = (fs == null ? 0 : fs.getN()) + (vs == null ? 0 : vs.getN());
            ep[leaf] = mass * Math.abs(v[leaf] - f[leaf]);
            weightSum += ep[leaf];
        }
        if(weightSum > 0) {
            for(int leaf = 0; leaf < leafCount; leaf++) {
                ep[leaf] /= weightSum;
            }
        }
        return new MeasureVector(f, v, ep);
    }

    private static double quantileOf(final KllDoublesSketch sketch, final double quantile) {
        return sketch == null || sketch.isEmpty() ? 0.0 : sketch.getQuantile(quantile);
    }

    /** Quantile of the union of the given slices' leaf sketches ({@code null} slices = all leaves). */
    private static double mergedQuantile(
            final LeafTable table,
            final int distribution,
            final List<Slice> slices,
            final double quantile,
            final boolean baseline) {

        final KllDoublesSketch merged = KllDoublesSketch.newHeapInstance(LeafTable.SKETCH_K);
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            if(slices == null || covered(slices, table.dims(leaf))) {
                final KllDoublesSketch sketch = baseline
                        ? table.baselineSketch(distribution, leaf)
                        : table.targetSketch(distribution, leaf);
                if(sketch != null && !sketch.isEmpty()) {
                    merged.merge(sketch);
                }
            }
        }
        return merged.isEmpty() ? 0.0 : merged.getQuantile(quantile);
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
