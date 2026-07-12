package com.mercari.solution.util.domain.attribution;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Adtributor: Bhagwan et al., "Adtributor: Revenue Debugging in Advertising Systems", NSDI 2014.
 * Ranks single dimensions by surprise (Jensen-Shannon divergence between baseline and target
 * distributions) and selects, per dimension, the values whose explanatory power exceeds
 * {@code teep} until the cumulative explanatory power exceeds {@code tep}.
 * Ported from the reference implementation github.com/shaido987/riskloc (algorithms/adtributor.py).
 */
public class Adtributor implements AttributionAlgorithm {

    @Override
    public List<Finding> localize(final LeafTable table, final MeasureVector measure, final EngineConfig config) {

        final double teep = config.adtributor().teep();
        final double tep = config.adtributor().tep();
        final double[] ep = measure.explanatoryPowers();
        final double[] f = measure.baseline();
        final double[] v = measure.target();
        final double baselineTotal = measure.baselineTotal();
        final double targetTotal = measure.targetTotal();

        record Element(String value, double ep, double surprise, double f, double v, int leafCount) {
        }

        final List<Finding> candidates = new ArrayList<>();
        for(int dim = 0; dim < table.dimensionCount(); dim++) {

            // Merge leaves per dimension value (TreeMap: deterministic value order)
            final Map<String, double[]> merged = new TreeMap<>();
            for(int leaf = 0; leaf < table.leafCount(); leaf++) {
                final double[] acc = merged.computeIfAbsent(table.dimValue(leaf, dim), k -> new double[4]);
                acc[0] += ep[leaf];
                acc[1] += f[leaf];
                acc[2] += v[leaf];
                acc[3] += 1;
            }
            final List<Element> elements = new ArrayList<>();
            for(final Map.Entry<String, double[]> entry : merged.entrySet()) {
                final double surprise = jsDivergence(
                        entry.getValue()[1], entry.getValue()[2], baselineTotal, targetTotal);
                elements.add(new Element(entry.getKey(), entry.getValue()[0], surprise,
                        entry.getValue()[1], entry.getValue()[2], (int) entry.getValue()[3]));
            }
            elements.sort(Comparator
                    .comparingDouble(Element::surprise).reversed()
                    .thenComparing(Element::value));

            // Accumulate values with ep > teep (in surprise order) until cumulative ep > tep.
            // Surprise of the candidate sums over all visited elements, matching the reference.
            final List<Element> selected = new ArrayList<>();
            double cumulativeEp = 0;
            double surpriseSum = 0;
            boolean found = false;
            for(final Element element : elements) {
                surpriseSum += element.surprise();
                if(element.ep() > teep) {
                    selected.add(element);
                    cumulativeEp += element.ep();
                    if(cumulativeEp > tep) {
                        found = true;
                        break;
                    }
                }
            }
            if(!found) {
                continue;
            }

            final List<Slice> slices = new ArrayList<>();
            double baselineSum = 0;
            double targetSum = 0;
            int leafCount = 0;
            for(final Element element : selected) {
                slices.add(new Slice(new int[]{dim}, new String[]{element.value()}));
                baselineSum += element.f();
                targetSum += element.v();
                leafCount += element.leafCount();
            }
            candidates.add(new Finding(slices, null, cumulativeEp, surpriseSum, baselineSum, targetSum, leafCount));
        }

        return candidates.stream()
                .sorted(Comparator.comparingDouble((Finding finding) -> finding.surprise()).reversed())
                .limit(config.topK())
                .toList();
    }

    /**
     * Jensen-Shannon divergence term of one element:
     * {@code 0.5 * (p*ln(2p/(p+q)) + q*ln(2q/(p+q)))} with non-finite terms treated as 0.
     */
    static double jsDivergence(final double f, final double v, final double baselineTotal, final double targetTotal) {
        final double p = baselineTotal == 0 ? 0 : f / baselineTotal;
        final double q = targetTotal == 0 ? 0 : v / targetTotal;
        final double pTerm = p * Math.log(2 * p / (p + q));
        final double qTerm = q * Math.log(2 * q / (p + q));
        return 0.5 * ((Double.isFinite(pTerm) ? pTerm : 0) + (Double.isFinite(qTerm) ? qTerm : 0));
    }
}
