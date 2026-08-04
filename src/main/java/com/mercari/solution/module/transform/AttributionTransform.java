package com.mercari.solution.module.transform;

import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.attribution.*;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketches;

import java.io.Serializable;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Attribution transform: explains the difference between two multi-dimensional aggregates
 * (baseline vs target) by localizing it to a concise set of dimension-value slices
 * (RiskLoc / Adtributor). Parameters follow the five concept blocks
 * measures / comparison / vocabulary / semantics / output plus the cross-cutting engine block;
 * enum values marked as future work in the specification are reserved in the schema and rejected
 * at validation time. The heavy lifting is done by the pure-Java core in
 * {@code com.mercari.solution.util.domain.attribution}.
 */
@Transform.Module(name="attribution")
public class AttributionTransform extends Transform {

    private enum MeasureType { fundamental, derived, distribution, distinct, sketch }
    private enum SketchFormat { kll, theta }
    private enum ComparisonMode { pair, series, cohort }
    private enum ReferenceStrategy { external, timeShift, split, synthetic }
    private enum SyntheticMethod { marginal, forecast }
    private enum VocabularyUnit { slice, metric }
    private enum DimensionType { flat, binned, hierarchy, embedding }
    private enum Expressiveness { slice, predicate, ruleList }
    private enum SemanticsBasis { contribution, mixRate, causalAdjusted }
    private enum Algorithm { riskloc, adtributor, squeeze, exhaustive }
    private enum FdrControl { none, bh }
    private enum OutputMode { report, featureSpec, interventionSpec }

    private static class Parameters implements Serializable {

        private List<MeasureParameter> measures;
        private ComparisonParameter comparison;
        private VocabularyParameter vocabulary;
        private SemanticsParameter semantics;
        private EngineParameter engine;
        private OutputParameter output;

        private static class MeasureParameter implements Serializable {
            private String name;
            private MeasureType type;
            private String expression;
            private List<Double> quantiles; // type: distribution / sketch(kll) only. default: [0.5]
            private SketchFormat format;    // type: sketch only. kll | theta
        }

        private static class ComparisonParameter implements Serializable {
            private ComparisonMode mode;
            private ReferenceParameter reference;
        }

        private static class ReferenceParameter implements Serializable {
            private ReferenceStrategy strategy;
            private String labelField;
            private String baselineLabel;
            private String targetLabel;
            private TimeShiftParameter timeShift;
            private SplitParameter split;
            private SyntheticParameter synthetic;
        }

        private static class TimeShiftParameter implements Serializable {
            private String offset;
            private String timeField;
        }

        private static class SplitParameter implements Serializable {
            private SplitByParameter by;
        }

        private static class SplitByParameter implements Serializable {
            private String field;
            private JsonElement baseline;
            private JsonElement target;
        }

        private static class SyntheticParameter implements Serializable {
            private SyntheticMethod method;
        }

        private static class VocabularyParameter implements Serializable {
            private VocabularyUnit unit;
            private List<DimensionParameter> dimensions;
            private Expressiveness expressiveness;
            private JsonElement candidates; // reserved for unit: metric
        }

        private static class DimensionParameter implements Serializable {
            private String name;
            private DimensionType type;
            private List<String> levels;    // reserved for type: hierarchy
            private String clusters;        // reserved for type: embedding
            private BinningParameter binning;
        }

        private static class BinningParameter implements Serializable {
            private DimensionSpec.Binning.Method method;
            private Integer bins;
        }

        private static class SemanticsParameter implements Serializable {
            private SemanticsBasis basis;
            private DerivedAllocation.Method derivedAllocation;
            private EngineConfig.EpBasis epBasis;
        }

        private static class EngineParameter implements Serializable {
            private Algorithm algorithm;
            private RiskLocParameter riskloc;
            private AdtributorParameter adtributor;
            private GuardsParameter guards;
        }

        private static class RiskLocParameter implements Serializable {
            private Double riskThreshold;
            private Double pepThreshold;
            private Integer pruningLayers;
        }

        private static class AdtributorParameter implements Serializable {
            private Double teep;
            private Double tep;
        }

        private static class GuardsParameter implements Serializable {
            private Double minSupport;
            private Integer maxLayer;
            private Integer maxCardinality;
            private FdrControl fdrControl;
        }

        private static class OutputParameter implements Serializable {
            private OutputMode mode;
            private Integer topK;
            private Boolean includeUncertainty; // reserved for bayesian algorithms
            private Boolean emitNoFinding;
        }

        private void validate(final String name, final MCollectionTuple inputs) {
            final String prefix = "attribution transform module[" + name + "] ";
            final List<String> errorMessages = new ArrayList<>();
            final Map<String, Schema> inputSchemas = inputs.getAllSchemaAsMap();

            validateReserved(prefix, errorMessages);
            validateMeasures(prefix, errorMessages, inputSchemas);
            validateDimensions(prefix, errorMessages, inputSchemas);
            validateReference(prefix, errorMessages, inputSchemas, inputs.size());
            validateEngineAndOutput(prefix, errorMessages);

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        // Rejections of enum values reserved for future versions. These subsume the spec's
        // cross-parameter constraints 1 (mixRate requires a derived measure), 2 (unit: metric
        // requires mode: series) and 5 (expressiveness above slice requires fdrControl):
        // re-instate those checks verbatim when the corresponding values are unlocked.
        // Constraint 6 (distribution measures cannot use shapley) lives in validateMeasures.
        private void validateReserved(final String prefix, final List<String> errorMessages) {
            if(comparison != null) {
                if(comparison.mode != null && !ComparisonMode.pair.equals(comparison.mode)) {
                    errorMessages.add(prefix + "comparison.mode: " + comparison.mode + " is reserved and not implemented yet");
                }
                if(comparison.reference != null
                        && comparison.reference.synthetic != null
                        && SyntheticMethod.forecast.equals(comparison.reference.synthetic.method)) {
                    errorMessages.add(prefix + "comparison.reference.synthetic.method: forecast is reserved and not implemented yet");
                }
            }
            if(vocabulary != null) {
                if(vocabulary.unit != null && !VocabularyUnit.slice.equals(vocabulary.unit)) {
                    errorMessages.add(prefix + "vocabulary.unit: " + vocabulary.unit + " is reserved and not implemented yet");
                }
                if(vocabulary.expressiveness != null && !Expressiveness.slice.equals(vocabulary.expressiveness)) {
                    errorMessages.add(prefix + "vocabulary.expressiveness: " + vocabulary.expressiveness + " is reserved and not implemented yet");
                }
                if(vocabulary.candidates != null && !vocabulary.candidates.isJsonNull()) {
                    errorMessages.add(prefix + "vocabulary.candidates is reserved for unit: metric and not implemented yet");
                }
                if(vocabulary.dimensions != null) {
                    for(final DimensionParameter dimension : vocabulary.dimensions) {
                        if(DimensionType.hierarchy.equals(dimension.type) || DimensionType.embedding.equals(dimension.type)) {
                            errorMessages.add(prefix + "vocabulary.dimensions.type: " + dimension.type + " is reserved and not implemented yet");
                        }
                    }
                }
            }
            if(semantics != null && semantics.basis != null && !SemanticsBasis.contribution.equals(semantics.basis)) {
                errorMessages.add(prefix + "semantics.basis: " + semantics.basis + " is reserved and not implemented yet");
            }
            if(engine != null) {
                if(Algorithm.squeeze.equals(engine.algorithm)) {
                    errorMessages.add(prefix + "engine.algorithm: squeeze is reserved and not implemented yet");
                }
                if(engine.guards != null && FdrControl.bh.equals(engine.guards.fdrControl)) {
                    errorMessages.add(prefix + "engine.guards.fdrControl: bh is reserved and not implemented yet");
                }
            }
            if(output != null) {
                if(output.mode != null && !OutputMode.report.equals(output.mode)) {
                    errorMessages.add(prefix + "output.mode: " + output.mode + " is reserved and not implemented yet");
                }
                if(Boolean.TRUE.equals(output.includeUncertainty)) {
                    errorMessages.add(prefix + "output.includeUncertainty is reserved and not implemented yet");
                }
            }
        }

        private void validateMeasures(
                final String prefix, final List<String> errorMessages, final Map<String, Schema> inputSchemas) {

            if(measures == null || measures.isEmpty()) {
                errorMessages.add(prefix + "measures parameter is required");
                return;
            }
            for(final MeasureParameter measure : measures) {
                if(measure.name == null || measure.name.isEmpty()) {
                    errorMessages.add(prefix + "measures.name parameter is required");
                    continue;
                }
                final MeasureType type = measure.type == null ? MeasureType.fundamental : measure.type;
                if(MeasureType.fundamental.equals(type)) {
                    if(measure.expression != null) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].expression must not be set for type: fundamental");
                    }
                    validateNumericField(prefix, errorMessages, inputSchemas, measure.name, "measures[" + measure.name + "]");
                } else if(MeasureType.derived.equals(type)) {
                    if(measure.expression == null || measure.expression.isEmpty()) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].expression parameter is required for type: derived");
                        continue;
                    }
                    final Set<String> variables;
                    try {
                        ExpressionUtil.createDefaultExpression(measure.expression);
                        variables = ExpressionUtil.estimateVariables(measure.expression);
                    } catch (final Throwable e) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].expression is invalid: " + e.getMessage());
                        continue;
                    }
                    if(variables.isEmpty()) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].expression must contain at least one variable");
                    }
                    for(final String variable : variables) {
                        validateNumericField(prefix, errorMessages, inputSchemas, variable,
                                "measures[" + measure.name + "].expression variable");
                    }
                    if(semantics != null
                            && DerivedAllocation.Method.shapley.equals(semantics.derivedAllocation)
                            && variables.size() > DerivedAllocation.MAX_SHAPLEY_VARIABLES) {
                        errorMessages.add(prefix + "measures[" + measure.name + "] has " + variables.size()
                                + " variables, but derivedAllocation: shapley supports at most "
                                + DerivedAllocation.MAX_SHAPLEY_VARIABLES);
                    }
                } else if(MeasureType.distribution.equals(type)) {
                    if(measure.expression != null) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].expression must not be set for type: distribution");
                    }
                    validateNumericField(prefix, errorMessages, inputSchemas, measure.name, "measures[" + measure.name + "]");
                    if(measure.quantiles != null) {
                        for(final Double quantile : measure.quantiles) {
                            if(quantile == null || !(quantile > 0 && quantile < 1)) {
                                errorMessages.add(prefix + "measures[" + measure.name
                                        + "].quantiles must be in (0, 1) exclusive: " + measure.quantiles);
                                break;
                            }
                        }
                    }
                    // Spec constraint 6
                    if(semantics != null && DerivedAllocation.Method.shapley.equals(semantics.derivedAllocation)) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "] type: distribution cannot be used with derivedAllocation: shapley");
                    }
                    // Quantiles are not additive: a net-change share is undefined for them
                    if(semantics != null && EngineConfig.EpBasis.netDelta.equals(semantics.epBasis)) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "] type: distribution always uses epBasis: absoluteDelta; remove epBasis: netDelta");
                    }
                    if(comparison != null && comparison.reference != null
                            && ReferenceStrategy.synthetic.equals(comparison.reference.strategy)) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "] type: distribution cannot be used with the synthetic reference"
                                + " (no independence model is defined for distributions)");
                    }
                } else if(MeasureType.distinct.equals(type)) {
                    if(measure.expression != null) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].expression must not be set for type: distinct");
                    }
                    if(measure.quantiles != null) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].quantiles must not be set for type: distinct");
                    }
                    // The identity column may be of any scalar type (string ids, numeric ids, ...)
                    validateFieldExists(prefix, errorMessages, inputSchemas, measure.name, "measures[" + measure.name + "]");
                    // Distinct estimates are not additive: a net-change share is undefined
                    if(semantics != null && EngineConfig.EpBasis.netDelta.equals(semantics.epBasis)) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "] type: distinct always uses epBasis: absoluteDelta; remove epBasis: netDelta");
                    }
                    if(comparison != null && comparison.reference != null
                            && ReferenceStrategy.synthetic.equals(comparison.reference.strategy)) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "] type: distinct cannot be used with the synthetic reference"
                                + " (no independence model is defined for identity sets)");
                    }
                } else if(MeasureType.sketch.equals(type)) {
                    if(measure.format == null) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "].format parameter is required for type: sketch (kll or theta)");
                        continue;
                    }
                    if(measure.expression != null) {
                        errorMessages.add(prefix + "measures[" + measure.name + "].expression must not be set for type: sketch");
                    }
                    validateSketchField(prefix, errorMessages, inputSchemas, measure.name, "measures[" + measure.name + "]");
                    if(SketchFormat.kll.equals(measure.format)) {
                        if(measure.quantiles != null) {
                            for(final Double quantile : measure.quantiles) {
                                if(quantile == null || !(quantile > 0 && quantile < 1)) {
                                    errorMessages.add(prefix + "measures[" + measure.name
                                            + "].quantiles must be in (0, 1) exclusive: " + measure.quantiles);
                                    break;
                                }
                            }
                        }
                        // Spec constraint 6 applies: a kll sketch measure is a distribution measure
                        if(semantics != null && DerivedAllocation.Method.shapley.equals(semantics.derivedAllocation)) {
                            errorMessages.add(prefix + "measures[" + measure.name
                                    + "] type: sketch (kll) cannot be used with derivedAllocation: shapley");
                        }
                    } else if(measure.quantiles != null) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "].quantiles must not be set for format: theta");
                    }
                    if(semantics != null && EngineConfig.EpBasis.netDelta.equals(semantics.epBasis)) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "] type: sketch always uses epBasis: absoluteDelta; remove epBasis: netDelta");
                    }
                    if(comparison != null && comparison.reference != null
                            && ReferenceStrategy.synthetic.equals(comparison.reference.strategy)) {
                        errorMessages.add(prefix + "measures[" + measure.name
                                + "] type: sketch cannot be used with the synthetic reference");
                    }
                }
            }
        }

        private void validateDimensions(
                final String prefix, final List<String> errorMessages, final Map<String, Schema> inputSchemas) {

            if(vocabulary == null || vocabulary.dimensions == null || vocabulary.dimensions.isEmpty()) {
                errorMessages.add(prefix + "vocabulary.dimensions parameter is required");
                return;
            }
            if(vocabulary.dimensions.size() > 31) {
                errorMessages.add(prefix + "vocabulary.dimensions supports at most 31 dimensions");
            }
            for(final DimensionParameter dimension : vocabulary.dimensions) {
                if(dimension.name == null || dimension.name.isEmpty()) {
                    errorMessages.add(prefix + "vocabulary.dimensions.name parameter is required");
                    continue;
                }
                for(final Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
                    if(!entry.getValue().hasField(dimension.name)) {
                        errorMessages.add(prefix + "vocabulary.dimensions[" + dimension.name
                                + "] field does not exist in input: " + entry.getKey());
                    }
                }
                final DimensionType type = dimension.type == null ? DimensionType.flat : dimension.type;
                if(DimensionType.binned.equals(type)) {
                    if(dimension.binning == null || dimension.binning.method == null || dimension.binning.bins == null) {
                        errorMessages.add(prefix + "vocabulary.dimensions[" + dimension.name
                                + "].binning parameter (method, bins) is required for type: binned");
                    } else if(dimension.binning.bins < 2) {
                        errorMessages.add(prefix + "vocabulary.dimensions[" + dimension.name
                                + "].binning.bins must be greater than 1");
                    }
                } else if(dimension.binning != null) {
                    errorMessages.add(prefix + "vocabulary.dimensions[" + dimension.name
                            + "].binning must not be set for type: " + type);
                }
                if(measures != null && measures.stream()
                        .anyMatch(m -> dimension.name.equals(m.name))) {
                    errorMessages.add(prefix + "vocabulary.dimensions[" + dimension.name
                            + "] must not also be declared as a measure");
                }
            }
        }

        private void validateReference(
                final String prefix, final List<String> errorMessages,
                final Map<String, Schema> inputSchemas, final int inputSize) {

            final ReferenceParameter reference = comparison == null ? null : comparison.reference;
            final ReferenceStrategy strategy = reference == null || reference.strategy == null
                    ? ReferenceStrategy.external : reference.strategy;

            switch (strategy) {
                case external -> {
                    if(reference == null || reference.labelField == null) {
                        if(inputSize != 2) {
                            errorMessages.add(prefix + "reference.strategy: external without labelField requires"
                                    + " exactly 2 inputs as [target, baseline], but got " + inputSize);
                        }
                    } else {
                        if(inputSize != 1) {
                            errorMessages.add(prefix + "reference.strategy: external with labelField requires"
                                    + " exactly 1 input, but got " + inputSize);
                        }
                        validateFieldExists(prefix, errorMessages, inputSchemas, reference.labelField, "reference.labelField");
                        if(reference.baselineLabel == null || reference.targetLabel == null) {
                            errorMessages.add(prefix + "reference.baselineLabel and reference.targetLabel parameters"
                                    + " are required when labelField is set");
                        }
                    }
                }
                case timeShift -> {
                    if(inputSize != 1) {
                        errorMessages.add(prefix + "reference.strategy: timeShift requires exactly 1 input, but got " + inputSize);
                    }
                    if(reference.timeShift == null || reference.timeShift.offset == null) {
                        errorMessages.add(prefix + "reference.timeShift.offset parameter is required");
                    } else {
                        try {
                            parseOffset(reference.timeShift.offset);
                        } catch (final IllegalArgumentException e) {
                            errorMessages.add(prefix + "reference.timeShift.offset is invalid: " + e.getMessage());
                        }
                    }
                    if(reference.timeShift != null && reference.timeShift.timeField != null) {
                        validateFieldExists(prefix, errorMessages, inputSchemas, reference.timeShift.timeField, "reference.timeShift.timeField");
                    }
                }
                case split -> {
                    if(inputSize != 1) {
                        errorMessages.add(prefix + "reference.strategy: split requires exactly 1 input, but got " + inputSize);
                    }
                    if(reference.split == null || reference.split.by == null
                            || reference.split.by.field == null
                            || isNullValue(reference.split.by.baseline)
                            || isNullValue(reference.split.by.target)) {
                        errorMessages.add(prefix + "reference.split.by parameters (field, baseline, target) are required");
                    } else {
                        validateFieldExists(prefix, errorMessages, inputSchemas, reference.split.by.field, "reference.split.by.field");
                    }
                }
                case synthetic -> {
                    if(inputSize != 1) {
                        errorMessages.add(prefix + "reference.strategy: synthetic requires exactly 1 input, but got " + inputSize);
                    }
                    // The marginal baseline preserves totals by construction, so the net-delta
                    // explanatory power is undefined; auto necessarily resolves to absoluteDelta
                    if(semantics != null && EngineConfig.EpBasis.netDelta.equals(semantics.epBasis)) {
                        errorMessages.add(prefix + "semantics.epBasis: netDelta cannot be used with"
                                + " reference.strategy: synthetic (the marginal baseline has zero net delta"
                                + " by construction); use absoluteDelta or auto");
                    }
                }
            }
        }

        private void validateEngineAndOutput(final String prefix, final List<String> errorMessages) {
            if(engine != null) {
                if(engine.riskloc != null) {
                    if(engine.riskloc.riskThreshold != null
                            && (engine.riskloc.riskThreshold <= 0 || engine.riskloc.riskThreshold > 1)) {
                        errorMessages.add(prefix + "engine.riskloc.riskThreshold must be in (0, 1]");
                    }
                    if(engine.riskloc.pepThreshold != null
                            && (engine.riskloc.pepThreshold < 0 || engine.riskloc.pepThreshold >= 1)) {
                        errorMessages.add(prefix + "engine.riskloc.pepThreshold must be in [0, 1)");
                    }
                    if(engine.riskloc.pruningLayers != null && engine.riskloc.pruningLayers < 0) {
                        errorMessages.add(prefix + "engine.riskloc.pruningLayers must not be negative");
                    }
                }
                if(engine.adtributor != null) {
                    if(engine.adtributor.teep != null && (engine.adtributor.teep <= 0 || engine.adtributor.teep >= 1)) {
                        errorMessages.add(prefix + "engine.adtributor.teep must be in (0, 1)");
                    }
                    if(engine.adtributor.tep != null && (engine.adtributor.tep <= 0 || engine.adtributor.tep >= 1)) {
                        errorMessages.add(prefix + "engine.adtributor.tep must be in (0, 1)");
                    }
                }
                if(engine.guards != null) {
                    if(engine.guards.minSupport != null
                            && (engine.guards.minSupport < 0 || engine.guards.minSupport >= 1)) {
                        errorMessages.add(prefix + "engine.guards.minSupport must be in [0, 1)");
                    }
                    if(engine.guards.maxLayer != null && engine.guards.maxLayer < 1) {
                        errorMessages.add(prefix + "engine.guards.maxLayer must be greater than 0");
                    }
                    if(engine.guards.maxCardinality != null && engine.guards.maxCardinality < 2) {
                        errorMessages.add(prefix + "engine.guards.maxCardinality must be greater than 1");
                    }
                }
            }
            if(output != null && output.topK != null && output.topK < 1) {
                errorMessages.add(prefix + "output.topK must be greater than 0");
            }
        }

        private static void validateNumericField(
                final String prefix, final List<String> errorMessages,
                final Map<String, Schema> inputSchemas, final String field, final String location) {

            for(final Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
                if(!entry.getValue().hasField(field)) {
                    errorMessages.add(prefix + location + " field: " + field
                            + " does not exist in input: " + entry.getKey());
                } else if(!isNumeric(entry.getValue().getField(field).getFieldType().getType())) {
                    errorMessages.add(prefix + location + " field: " + field
                            + " must be a numeric type in input: " + entry.getKey());
                }
            }
        }

        private static void validateFieldExists(
                final String prefix, final List<String> errorMessages,
                final Map<String, Schema> inputSchemas, final String field, final String location) {

            for(final Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
                if(!entry.getValue().hasField(field)) {
                    errorMessages.add(prefix + location + " field: " + field
                            + " does not exist in input: " + entry.getKey());
                }
            }
        }

        private static void validateSketchField(
                final String prefix, final List<String> errorMessages,
                final Map<String, Schema> inputSchemas, final String field, final String location) {

            for(final Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
                if(!entry.getValue().hasField(field)) {
                    errorMessages.add(prefix + location + " field: " + field
                            + " does not exist in input: " + entry.getKey());
                } else {
                    final Schema.Type fieldType = entry.getValue().getField(field).getFieldType().getType();
                    if(!Schema.Type.bytes.equals(fieldType) && !Schema.Type.string.equals(fieldType)) {
                        errorMessages.add(prefix + location + " field: " + field
                                + " must be a bytes (serialized sketch) or string (base64) type in input: "
                                + entry.getKey());
                    }
                }
            }
        }

        private static boolean isNumeric(final Schema.Type type) {
            return switch (type) {
                case int8, int16, int32, int64, float8, float16, float32, float64, decimal -> true;
                default -> false;
            };
        }

        private static boolean isNullValue(final JsonElement element) {
            return element == null || element.isJsonNull();
        }

        private void setDefaults() {
            for(final MeasureParameter measure : measures) {
                if(measure.type == null) {
                    measure.type = MeasureType.fundamental;
                }
                if((MeasureType.distribution.equals(measure.type)
                        || (MeasureType.sketch.equals(measure.type) && SketchFormat.kll.equals(measure.format)))
                        && (measure.quantiles == null || measure.quantiles.isEmpty())) {
                    measure.quantiles = List.of(0.5);
                }
            }
            if(comparison == null) {
                comparison = new ComparisonParameter();
            }
            if(comparison.mode == null) {
                comparison.mode = ComparisonMode.pair;
            }
            if(comparison.reference == null) {
                comparison.reference = new ReferenceParameter();
            }
            if(comparison.reference.strategy == null) {
                comparison.reference.strategy = ReferenceStrategy.external;
            }
            if(comparison.reference.synthetic != null && comparison.reference.synthetic.method == null) {
                comparison.reference.synthetic.method = SyntheticMethod.marginal;
            }
            for(final DimensionParameter dimension : vocabulary.dimensions) {
                if(dimension.type == null) {
                    dimension.type = DimensionType.flat;
                }
            }
            if(vocabulary.unit == null) {
                vocabulary.unit = VocabularyUnit.slice;
            }
            if(vocabulary.expressiveness == null) {
                vocabulary.expressiveness = Expressiveness.slice;
            }
            if(semantics == null) {
                semantics = new SemanticsParameter();
            }
            if(semantics.basis == null) {
                semantics.basis = SemanticsBasis.contribution;
            }
            if(semantics.derivedAllocation == null) {
                semantics.derivedAllocation = DerivedAllocation.Method.gre;
            }
            if(semantics.epBasis == null) {
                semantics.epBasis = EngineConfig.EpBasis.auto;
            }
            if(engine == null) {
                engine = new EngineParameter();
            }
            if(engine.algorithm == null) {
                engine.algorithm = Algorithm.riskloc;
            }
            if(engine.riskloc == null) {
                engine.riskloc = new RiskLocParameter();
            }
            if(engine.riskloc.riskThreshold == null) {
                engine.riskloc.riskThreshold = 0.5;
            }
            if(engine.riskloc.pepThreshold == null) {
                engine.riskloc.pepThreshold = 0.02;
            }
            if(engine.riskloc.pruningLayers == null) {
                engine.riskloc.pruningLayers = 1;
            }
            if(engine.adtributor == null) {
                engine.adtributor = new AdtributorParameter();
            }
            if(engine.adtributor.teep == null) {
                engine.adtributor.teep = 0.1;
            }
            if(engine.adtributor.tep == null) {
                engine.adtributor.tep = 0.67;
            }
            if(engine.guards == null) {
                engine.guards = new GuardsParameter();
            }
            if(engine.guards.minSupport == null) {
                engine.guards.minSupport = 0.005;
            }
            if(engine.guards.maxLayer == null) {
                engine.guards.maxLayer = 3;
            }
            if(engine.guards.maxCardinality == null) {
                engine.guards.maxCardinality = 200;
            }
            if(engine.guards.fdrControl == null) {
                engine.guards.fdrControl = FdrControl.none;
            }
            if(output == null) {
                output = new OutputParameter();
            }
            if(output.mode == null) {
                output.mode = OutputMode.report;
            }
            if(output.topK == null) {
                output.topK = 3;
            }
            if(output.emitNoFinding == null) {
                output.emitNoFinding = true;
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName(), inputs);
        parameters.setDefaults();

        // Constraint 4 of the spec: the batch reference strategies cannot close within a window.
        // Streaming support (timeShift via state, external via side input) is a future version.
        if(OptionUtil.isStreaming(inputs)) {
            throw new IllegalModuleException("attribution transform module[" + getName()
                    + "] does not support streaming mode yet");
        }

        final Task task = Task.of(parameters, inputs.getAllInputs());
        final Schema outputSchema = createOutputSchema();

        // Input rows may be raw events or pre-aggregated leaves: leaf aggregation (sums, KLL and
        // Theta sketches, row counts) runs distributed per dimension tuple with combiner lifting,
        // so only the distinct leaf aggregates reach the single-worker localization step.
        final PCollection<MElement> unioned = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        // timeShift anchors its two comparison windows at the max time in the data,
        // computed up-front as a side input so runs stay deterministic and reproducible
        PCollectionView<List<Long>> maxTimeView = null;
        if(ReferenceStrategy.timeShift.equals(task.strategy)) {
            maxTimeView = unioned
                    .apply("ExtractEventTime", ParDo.of(new ExtractTimeDoFn(task)))
                    .apply("MaxEventTime", Max.longsGlobally().withoutDefaults())
                    .apply("MaxEventTimeView", View.asList());
        }

        final TupleTag<KV<List<String>, LeafContribution>> contributionTag = new TupleTag<>() {};
        final TupleTag<BadRecord> resolveFailureTag = new TupleTag<>() {};
        ParDo.MultiOutput<MElement, KV<List<String>, LeafContribution>> resolvePar = ParDo
                .of(new ContributionDoFn(task, getLoggings(), getFailFast(), resolveFailureTag, maxTimeView))
                .withOutputTags(contributionTag, TupleTagList.of(resolveFailureTag));
        if(maxTimeView != null) {
            resolvePar = resolvePar.withSideInputs(maxTimeView);
        }
        final PCollectionTuple resolved = unioned.apply("ResolveRoles", resolvePar);

        final KvCoder<List<String>, LeafAggregate> leafCoder = KvCoder.of(
                ListCoder.of(StringUtf8Coder.of()), SerializableCoder.of(LeafAggregate.class));
        final PCollection<KV<List<String>, LeafAggregate>> leaves = resolved.get(contributionTag)
                .setCoder(KvCoder.of(
                        ListCoder.of(StringUtf8Coder.of()), SerializableCoder.of(LeafContribution.class)))
                .apply("AggregateLeaves", Combine.perKey(new LeafCombineFn(
                        task.columnNames.size(), task.distributionNames.size(), task.distinctNames.size())))
                .setCoder(leafCoder);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final PCollectionTuple outputs = leaves
                .apply("WithGatherKey", WithKeys.of(""))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), leafCoder))
                .apply("GatherLeaves", GroupByKey.create())
                .apply("Attribute", ParDo
                        .of(new AttributionDoFn(task, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        if(errorHandler != null) {
            errorHandler.addError(resolved.get(resolveFailureTag));
            errorHandler.addError(outputs.get(failureTag));
        }

        return MCollectionTuple
                .of(outputs.get(outputTag).setCoder(ElementCoder.of(outputSchema)), outputSchema);
    }

    /** Serializable execution spec derived from the validated parameters. */
    private static class Task implements Serializable {

        private List<DimensionSpec> dimensions;
        private List<MeasureSpec> measures;
        private List<String> columnNames;
        private List<String> distributionNames;
        private List<String> distinctNames;
        // Parallel to distributionNames/distinctNames: true when the column is fed by
        // pre-serialized sketch bytes (measures.type: sketch) instead of raw samples/identities
        private boolean[] distributionFromSketch;
        private boolean[] distinctFromSketch;
        private EngineConfig engineConfig;
        private String algorithm;

        private ReferenceStrategy strategy;
        private String labelField;
        private String baselineLabel;
        private String targetLabel;
        private long timeShiftMillis;
        private String timeField;
        private String splitField;
        private String splitBaseline;
        private String splitTarget;
        private boolean syntheticMarginal;

        private boolean emitNoFinding;

        private static Task of(final Parameters parameters, final List<String> inputNames) {
            final Task task = new Task();

            task.dimensions = parameters.vocabulary.dimensions.stream()
                    .map(dimension -> DimensionType.binned.equals(dimension.type)
                            ? DimensionSpec.binned(dimension.name, dimension.binning.method, dimension.binning.bins)
                            : DimensionSpec.flat(dimension.name))
                    .toList();

            final Set<String> columnNames = new LinkedHashSet<>();
            final List<String> distributionNames = new ArrayList<>();
            final List<String> distinctNames = new ArrayList<>();
            final List<Boolean> distributionFromSketch = new ArrayList<>();
            final List<Boolean> distinctFromSketch = new ArrayList<>();
            final List<MeasureSpec> measures = new ArrayList<>();
            for(final Parameters.MeasureParameter measure : parameters.measures) {
                if(MeasureType.derived.equals(measure.type)) {
                    final List<String> variables = new ArrayList<>(
                            new TreeSet<>(ExpressionUtil.estimateVariables(measure.expression)));
                    measures.add(MeasureSpec.derived(measure.name, measure.expression, variables));
                    columnNames.addAll(variables);
                } else if(MeasureType.distribution.equals(measure.type)) {
                    measures.add(MeasureSpec.distribution(measure.name, measure.quantiles));
                    distributionNames.add(measure.name);
                    distributionFromSketch.add(false);
                } else if(MeasureType.distinct.equals(measure.type)) {
                    measures.add(MeasureSpec.distinct(measure.name));
                    distinctNames.add(measure.name);
                    distinctFromSketch.add(false);
                } else if(MeasureType.sketch.equals(measure.type)) {
                    // Pre-serialized sketch input: joins the distribution / distinct column family
                    // of the core depending on the sketch format — the engine sees no difference
                    if(SketchFormat.kll.equals(measure.format)) {
                        measures.add(MeasureSpec.distribution(measure.name, measure.quantiles));
                        distributionNames.add(measure.name);
                        distributionFromSketch.add(true);
                    } else {
                        measures.add(MeasureSpec.distinct(measure.name));
                        distinctNames.add(measure.name);
                        distinctFromSketch.add(true);
                    }
                } else {
                    measures.add(MeasureSpec.fundamental(measure.name));
                    columnNames.add(measure.name);
                }
            }
            task.measures = measures;
            task.columnNames = new ArrayList<>(columnNames);
            task.distributionNames = distributionNames;
            task.distinctNames = distinctNames;
            task.distributionFromSketch = toArray(distributionFromSketch);
            task.distinctFromSketch = toArray(distinctFromSketch);

            task.algorithm = parameters.engine.algorithm.name();
            task.engineConfig = new EngineConfig(
                    EngineConfig.Algorithm.valueOf(parameters.engine.algorithm.name()),
                    new EngineConfig.RiskLocParams(
                            parameters.engine.riskloc.riskThreshold,
                            parameters.engine.riskloc.pepThreshold,
                            parameters.engine.riskloc.pruningLayers),
                    new EngineConfig.AdtributorParams(
                            parameters.engine.adtributor.teep,
                            parameters.engine.adtributor.tep),
                    new EngineConfig.Guards(
                            parameters.engine.guards.minSupport,
                            parameters.engine.guards.maxLayer,
                            parameters.engine.guards.maxCardinality),
                    parameters.semantics.derivedAllocation,
                    parameters.semantics.epBasis,
                    parameters.output.topK);

            final Parameters.ReferenceParameter reference = parameters.comparison.reference;
            task.strategy = reference.strategy;
            switch (reference.strategy) {
                case external -> {
                    task.labelField = reference.labelField;
                    task.baselineLabel = reference.baselineLabel;
                    task.targetLabel = reference.targetLabel;
                }
                case timeShift -> {
                    task.timeShiftMillis = parseOffset(reference.timeShift.offset).toMillis();
                    task.timeField = reference.timeShift.timeField;
                }
                case split -> {
                    task.splitField = reference.split.by.field;
                    task.splitBaseline = reference.split.by.baseline.getAsString();
                    task.splitTarget = reference.split.by.target.getAsString();
                }
                case synthetic -> task.syntheticMarginal = true;
            }

            task.emitNoFinding = parameters.output.emitNoFinding;
            return task;
        }

        private static boolean[] toArray(final List<Boolean> flags) {
            final boolean[] array = new boolean[flags.size()];
            for(int i = 0; i < flags.size(); i++) {
                array[i] = flags.get(i);
            }
            return array;
        }
    }

    private enum Role { TARGET, BASELINE, DROP }

    /** Key marker for the target role in the leaf aggregation key (first key element). */
    private static final String ROLE_TARGET = "t";
    private static final String ROLE_BASELINE = "b";

    private static Long extractEpochMillis(final Task task, final MElement element) {
        if(task.timeField == null) {
            return element.getEpochMillis();
        }
        final org.joda.time.Instant instant = element.getAsJodaInstant(task.timeField);
        return instant == null ? null : instant.getMillis();
    }

    private static class ExtractTimeDoFn extends DoFn<MElement, Long> {

        private final Task task;

        ExtractTimeDoFn(final Task task) {
            this.task = task;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Long epochMillis = extractEpochMillis(task, c.element());
            if(epochMillis != null) {
                c.output(epochMillis);
            }
        }
    }

    /** Per-row contribution to one leaf: measure sums, distribution samples and identities. */
    static class LeafContribution implements Serializable {

        final double[] columns;      // measure column values (0.0 for missing)
        final double[] samples;      // distribution samples (NaN = absent)
        final String[] identities;   // distinct identities (null = absent)
        final byte[][] kllSketches;   // pre-serialized KLL sketches per distribution column (null = absent)
        final byte[][] thetaSketches; // pre-serialized Theta sketches per distinct column (null = absent)

        LeafContribution(final double[] columns, final double[] samples, final String[] identities) {
            this(columns, samples, identities,
                    new byte[samples.length][], new byte[identities.length][]);
        }

        LeafContribution(
                final double[] columns,
                final double[] samples,
                final String[] identities,
                final byte[][] kllSketches,
                final byte[][] thetaSketches) {
            this.columns = columns;
            this.samples = samples;
            this.identities = identities;
            this.kllSketches = kllSketches;
            this.thetaSketches = thetaSketches;
        }
    }

    /** Resolves each row's comparison role and emits its keyed leaf contribution. */
    private static class ContributionDoFn extends DoFn<MElement, KV<List<String>, LeafContribution>> {

        private final Task task;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;
        private final PCollectionView<List<Long>> maxTimeView;
        private final Counter droppedRows = Metrics.counter(AttributionTransform.class, "attributionDroppedRows");

        ContributionDoFn(
                final Task task,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag,
                final PCollectionView<List<Long>> maxTimeView) {

            this.task = task;
            this.logs = Logging.map(logs);
            this.failFast = failFast;
            this.failureTag = failureTag;
            this.maxTimeView = maxTimeView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            try {
                Logging.log(LOG, logs, "input", element);
                Long maxEpochMillis = null;
                if(maxTimeView != null) {
                    final List<Long> maxTimes = c.sideInput(maxTimeView);
                    if(maxTimes.isEmpty()) {
                        droppedRows.inc();
                        return;
                    }
                    maxEpochMillis = maxTimes.getFirst();
                }
                final Role role = resolve(element, maxEpochMillis);
                if(Role.DROP.equals(role)) {
                    droppedRows.inc();
                    return;
                }

                final List<String> dimensionNames = DimensionSpec.names(task.dimensions);
                final List<String> key = new ArrayList<>(dimensionNames.size() + 1);
                key.add(Role.TARGET.equals(role) ? ROLE_TARGET : ROLE_BASELINE);
                for(final String dimension : dimensionNames) {
                    final String value = element.getAsString(dimension);
                    key.add(value == null ? LeafTable.NULL_VALUE : value);
                }

                final double[] columns = new double[task.columnNames.size()];
                for(int i = 0; i < columns.length; i++) {
                    final Double value = element.getAsDouble(task.columnNames.get(i));
                    columns[i] = value == null ? 0.0 : value;
                }
                final double[] samples = new double[task.distributionNames.size()];
                final byte[][] kllSketches = new byte[task.distributionNames.size()][];
                for(int i = 0; i < samples.length; i++) {
                    if(task.distributionFromSketch[i]) {
                        samples[i] = Double.NaN;
                        final byte[] bytes = readSketchBytes(element, task.distributionNames.get(i));
                        if(bytes != null) {
                            // Validate here so corrupt sketches route to the failure output
                            // (deserialization failures inside Combine would fail the pipeline)
                            KllDoublesSketch.heapify(Memory.wrap(bytes));
                            kllSketches[i] = bytes;
                        }
                    } else {
                        final Double sample = element.getAsDouble(task.distributionNames.get(i));
                        samples[i] = sample == null ? Double.NaN : sample;
                    }
                }
                final String[] identities = new String[task.distinctNames.size()];
                final byte[][] thetaSketches = new byte[task.distinctNames.size()][];
                for(int i = 0; i < identities.length; i++) {
                    if(task.distinctFromSketch[i]) {
                        final byte[] bytes = readSketchBytes(element, task.distinctNames.get(i));
                        if(bytes != null) {
                            Sketches.heapifySketch(Memory.wrap(bytes));
                            thetaSketches[i] = bytes;
                        }
                    } else {
                        identities[i] = element.getAsString(task.distinctNames.get(i));
                    }
                }
                c.output(KV.of(key, new LeafContribution(columns, samples, identities, kllSketches, thetaSketches)));
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to resolve attribution role", element, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        /** Reads serialized sketch bytes from a bytes field, or base64 from a string field. */
        private static byte[] readSketchBytes(final MElement element, final String field) {
            final Object value = element.getPrimitiveValue(field);
            return switch (value) {
                case null -> null;
                case java.nio.ByteBuffer buffer -> {
                    final byte[] bytes = new byte[buffer.remaining()];
                    buffer.asReadOnlyBuffer().get(bytes);
                    yield bytes;
                }
                case byte[] bytes -> bytes;
                case String base64 -> base64.isEmpty() ? null : java.util.Base64.getDecoder().decode(base64);
                case org.apache.avro.util.Utf8 utf8 -> {
                    final String base64 = utf8.toString();
                    yield base64.isEmpty() ? null : java.util.Base64.getDecoder().decode(base64);
                }
                default -> throw new IllegalArgumentException(
                        "sketch field " + field + " has unsupported value type: " + value.getClass());
            };
        }

        private Role resolve(final MElement element, final Long maxEpochMillis) {
            return switch (task.strategy) {
                case external -> {
                    if(task.labelField == null) {
                        // 2-input form: config inputs order is [target, baseline]
                        yield element.getIndex() == 0 ? Role.TARGET : Role.BASELINE;
                    }
                    final String label = element.getAsString(task.labelField);
                    if(task.targetLabel.equals(label)) {
                        yield Role.TARGET;
                    } else if(task.baselineLabel.equals(label)) {
                        yield Role.BASELINE;
                    }
                    yield Role.DROP;
                }
                case split -> {
                    final String value = element.getAsString(task.splitField);
                    if(task.splitTarget.equals(value)) {
                        yield Role.TARGET;
                    } else if(task.splitBaseline.equals(value)) {
                        yield Role.BASELINE;
                    }
                    yield Role.DROP;
                }
                case timeShift -> {
                    final Long epochMillis = extractEpochMillis(task, element);
                    if(epochMillis == null) {
                        yield Role.DROP;
                    }
                    if(epochMillis > maxEpochMillis - task.timeShiftMillis) {
                        yield Role.TARGET;
                    } else if(epochMillis > maxEpochMillis - 2 * task.timeShiftMillis) {
                        yield Role.BASELINE;
                    }
                    yield Role.DROP;
                }
                case synthetic -> Role.TARGET;
            };
        }
    }

    /** Distributed per-leaf aggregate: row count, measure sums and serialized sketches. */
    static class LeafAggregate implements Serializable {

        final long rows;
        final double[] sums;
        final byte[][] kll;    // serialized KLL sketches per distribution column (null = empty)
        final byte[][] theta;  // serialized Theta sketches per distinct column (null = empty)

        LeafAggregate(final long rows, final double[] sums, final byte[][] kll, final byte[][] theta) {
            this.rows = rows;
            this.sums = sums;
            this.kll = kll;
            this.theta = theta;
        }
    }

    /** Combines leaf contributions with combiner lifting; sketches accumulate on the mappers. */
    static class LeafCombineFn extends Combine.CombineFn<LeafContribution, LeafCombineFn.Accumulator, LeafAggregate> {

        private final int columnCount;
        private final int distributionCount;
        private final int distinctCount;

        LeafCombineFn(final int columnCount, final int distributionCount, final int distinctCount) {
            this.columnCount = columnCount;
            this.distributionCount = distributionCount;
            this.distinctCount = distinctCount;
        }

        static class Accumulator implements Serializable {

            long rows;
            double[] sums;
            transient KllDoublesSketch[] kll;
            transient org.apache.datasketches.theta.Union[] theta;

            Accumulator(final int columnCount, final int distributionCount, final int distinctCount) {
                this.rows = 0;
                this.sums = new double[columnCount];
                this.kll = new KllDoublesSketch[distributionCount];
                this.theta = new org.apache.datasketches.theta.Union[distinctCount];
            }

            private void writeObject(final java.io.ObjectOutputStream out) throws java.io.IOException {
                out.defaultWriteObject();
                out.writeInt(kll.length);
                for(final KllDoublesSketch sketch : kll) {
                    writeBytes(out, sketch == null || sketch.isEmpty() ? null : sketch.toByteArray());
                }
                out.writeInt(theta.length);
                for(final org.apache.datasketches.theta.Union union : theta) {
                    writeBytes(out, union == null ? null : union.getResult().toByteArray());
                }
            }

            private void readObject(final java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
                in.defaultReadObject();
                this.kll = new KllDoublesSketch[in.readInt()];
                for(int i = 0; i < kll.length; i++) {
                    final byte[] bytes = readBytes(in);
                    kll[i] = bytes == null ? null : KllDoublesSketch.heapify(Memory.wrap(bytes));
                }
                this.theta = new org.apache.datasketches.theta.Union[in.readInt()];
                for(int i = 0; i < theta.length; i++) {
                    final byte[] bytes = readBytes(in);
                    if(bytes != null) {
                        theta[i] = newUnion();
                        theta[i].union(Sketches.heapifySketch(Memory.wrap(bytes)));
                    }
                }
            }

            private static void writeBytes(final java.io.ObjectOutputStream out, final byte[] bytes) throws java.io.IOException {
                out.writeInt(bytes == null ? -1 : bytes.length);
                if(bytes != null) {
                    out.write(bytes);
                }
            }

            private static byte[] readBytes(final java.io.ObjectInputStream in) throws java.io.IOException {
                final int length = in.readInt();
                if(length < 0) {
                    return null;
                }
                final byte[] bytes = new byte[length];
                in.readFully(bytes);
                return bytes;
            }
        }

        private static org.apache.datasketches.theta.Union newUnion() {
            return SetOperation.builder().setLogNominalEntries(LeafTable.THETA_LG_K).buildUnion();
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator(columnCount, distributionCount, distinctCount);
        }

        @Override
        public Accumulator addInput(final Accumulator acc, final LeafContribution input) {
            acc.rows++;
            for(int i = 0; i < columnCount; i++) {
                if(!Double.isNaN(input.columns[i])) {
                    acc.sums[i] += input.columns[i];
                }
            }
            for(int i = 0; i < distributionCount; i++) {
                final double sample = input.samples[i];
                if(Double.isFinite(sample)) {
                    if(acc.kll[i] == null) {
                        acc.kll[i] = KllDoublesSketch.newHeapInstance(LeafTable.SKETCH_K);
                    }
                    acc.kll[i].update(sample);
                }
            }
            for(int i = 0; i < distinctCount; i++) {
                final String identity = input.identities[i];
                if(identity != null) {
                    if(acc.theta[i] == null) {
                        acc.theta[i] = newUnion();
                    }
                    acc.theta[i].update(identity);
                }
            }
            // Pre-serialized sketch contributions (measures.type: sketch) merge wholesale
            for(int i = 0; i < distributionCount; i++) {
                if(input.kllSketches[i] != null) {
                    if(acc.kll[i] == null) {
                        acc.kll[i] = KllDoublesSketch.newHeapInstance(LeafTable.SKETCH_K);
                    }
                    acc.kll[i].merge(KllDoublesSketch.heapify(Memory.wrap(input.kllSketches[i])));
                }
            }
            for(int i = 0; i < distinctCount; i++) {
                if(input.thetaSketches[i] != null) {
                    if(acc.theta[i] == null) {
                        acc.theta[i] = newUnion();
                    }
                    acc.theta[i].union(Sketches.heapifySketch(Memory.wrap(input.thetaSketches[i])));
                }
            }
            return acc;
        }

        @Override
        public Accumulator mergeAccumulators(final Iterable<Accumulator> accumulators) {
            Accumulator merged = null;
            for(final Accumulator acc : accumulators) {
                if(merged == null) {
                    merged = acc;
                    continue;
                }
                merged.rows += acc.rows;
                for(int i = 0; i < columnCount; i++) {
                    merged.sums[i] += acc.sums[i];
                }
                for(int i = 0; i < distributionCount; i++) {
                    if(acc.kll[i] == null || acc.kll[i].isEmpty()) {
                        continue;
                    }
                    if(merged.kll[i] == null) {
                        merged.kll[i] = KllDoublesSketch.newHeapInstance(LeafTable.SKETCH_K);
                    }
                    merged.kll[i].merge(acc.kll[i]);
                }
                for(int i = 0; i < distinctCount; i++) {
                    if(acc.theta[i] == null) {
                        continue;
                    }
                    if(merged.theta[i] == null) {
                        merged.theta[i] = newUnion();
                    }
                    merged.theta[i].union(acc.theta[i].getResult());
                }
            }
            return merged == null ? createAccumulator() : merged;
        }

        @Override
        public LeafAggregate extractOutput(final Accumulator acc) {
            final byte[][] kll = new byte[distributionCount][];
            for(int i = 0; i < distributionCount; i++) {
                kll[i] = acc.kll[i] == null || acc.kll[i].isEmpty() ? null : acc.kll[i].toByteArray();
            }
            final byte[][] theta = new byte[distinctCount][];
            for(int i = 0; i < distinctCount; i++) {
                theta[i] = acc.theta[i] == null ? null : acc.theta[i].getResult().toByteArray();
            }
            return new LeafAggregate(acc.rows, acc.sums, kll, theta);
        }

        @Override
        public org.apache.beam.sdk.coders.Coder<Accumulator> getAccumulatorCoder(
                final org.apache.beam.sdk.coders.CoderRegistry registry,
                final org.apache.beam.sdk.coders.Coder<LeafContribution> inputCoder) {
            return SerializableCoder.of(Accumulator.class);
        }
    }

    private static class AttributionDoFn extends DoFn<KV<String, Iterable<KV<List<String>, LeafAggregate>>>, MElement> {

        private final Task task;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;
        private final Schema outputSchema;

        AttributionDoFn(
                final Task task,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.task = task;
            this.logs = Logging.map(logs);
            this.failFast = failFast;
            this.failureTag = failureTag;
            this.outputSchema = createOutputSchema();
        }

        @Setup
        public void setup() {
            this.outputSchema.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            try {
                final List<String> dimensionNames = DimensionSpec.names(task.dimensions);
                final LeafTable.Builder builder = LeafTable
                        .builder(dimensionNames, task.columnNames, task.distributionNames, task.distinctNames);
                for(final KV<List<String>, LeafAggregate> leaf : c.element().getValue()) {
                    final List<String> key = leaf.getKey();
                    final boolean target = ROLE_TARGET.equals(key.getFirst());
                    final String[] dims = key.subList(1, key.size()).toArray(new String[0]);
                    final LeafAggregate aggregate = leaf.getValue();
                    if(target) {
                        builder.addTarget(dims, aggregate.sums);
                        builder.addTargetRows(dims, aggregate.rows);
                    } else {
                        builder.addBaseline(dims, aggregate.sums);
                        builder.addBaselineRows(dims, aggregate.rows);
                    }
                    for(int d = 0; d < task.distributionNames.size(); d++) {
                        if(aggregate.kll[d] == null) {
                            continue;
                        }
                        final KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(aggregate.kll[d]));
                        if(target) {
                            builder.addTargetSketch(dims, d, sketch);
                        } else {
                            builder.addBaselineSketch(dims, d, sketch);
                        }
                    }
                    for(int d = 0; d < task.distinctNames.size(); d++) {
                        if(aggregate.theta[d] == null) {
                            continue;
                        }
                        final org.apache.datasketches.theta.Sketch sketch =
                                Sketches.heapifySketch(Memory.wrap(aggregate.theta[d]));
                        if(target) {
                            builder.addTargetDistinct(dims, d, sketch);
                        } else {
                            builder.addBaselineDistinct(dims, d, sketch);
                        }
                    }
                }
                if(builder.isEmpty()) {
                    return;
                }

                final AttributionResult result = AttributionEngine.run(
                        builder.build(), task.dimensions, task.measures, task.engineConfig, task.syntheticMarginal);

                for(final MeasureResult measureResult : result.results()) {
                    int rank = 1;
                    for(final Finding finding : measureResult.findings()) {
                        final MElement output = createFindingElement(measureResult, finding, rank++, c.timestamp());
                        Logging.log(LOG, logs, "output", output);
                        c.output(output);
                    }
                    if(measureResult.findings().isEmpty() && task.emitNoFinding) {
                        final MElement output = createNoFindingElement(measureResult, c.timestamp());
                        Logging.log(LOG, logs, "output", output);
                        c.output(output);
                    }
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to run attribution", Map.of("groupKey", c.element().getKey()), e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        private MElement createFindingElement(
                final MeasureResult measureResult,
                final Finding finding,
                final int rank,
                final org.joda.time.Instant timestamp) {

            final List<MElement> elements = new ArrayList<>();
            for(final Slice slice : finding.slices()) {
                for(int i = 0; i < slice.dims().length; i++) {
                    elements.add(MElement.builder()
                            .withString("dimension", DimensionSpec.names(task.dimensions).get(slice.dims()[i]))
                            .withString("value", slice.values()[i])
                            .build());
                }
            }
            final double delta = finding.targetSum() - finding.baselineSum();
            final MElement.Builder builder = MElement.builder()
                    .withString("measure", measureResult.measure())
                    .withString("algorithm", task.algorithm)
                    .withString("epBasis", measureResult.epBasis().name())
                    .withInt64("rank", (long) rank)
                    .withElementList("elements", elements)
                    .withInt64("layer", (long) finding.layer())
                    .withFloat64("explanatoryPower", finding.explanatoryPower())
                    .withFloat64("baseline", finding.baselineSum())
                    .withFloat64("target", finding.targetSum())
                    .withFloat64("delta", delta)
                    .withFloat64("totalBaseline", measureResult.baselineTotal())
                    .withFloat64("totalTarget", measureResult.targetTotal())
                    .withInt64("leafCount", (long) finding.leafCount())
                    .withBool("noFinding", false);
            if(measureResult.quantile() != null) {
                builder.withFloat64("quantile", measureResult.quantile());
            }
            if(finding.riskScore() != null) {
                builder.withFloat64("riskScore", finding.riskScore());
            }
            if(finding.surprise() != null) {
                builder.withFloat64("surprise", finding.surprise());
            }
            if(finding.baselineSum() != 0) {
                builder.withFloat64("deltaRatio", delta / finding.baselineSum());
            }
            return builder.withEventTime(timestamp).build();
        }

        private MElement createNoFindingElement(
                final MeasureResult measureResult,
                final org.joda.time.Instant timestamp) {

            final MElement.Builder builder = MElement.builder();
            if(measureResult.quantile() != null) {
                builder.withFloat64("quantile", measureResult.quantile());
            }
            return builder
                    .withString("measure", measureResult.measure())
                    .withString("algorithm", task.algorithm)
                    .withString("epBasis", measureResult.epBasis().name())
                    .withInt64("rank", 0L)
                    .withElementList("elements", List.of())
                    .withInt64("layer", 0L)
                    .withFloat64("explanatoryPower", 0D)
                    .withFloat64("baseline", 0D)
                    .withFloat64("target", 0D)
                    .withFloat64("delta", 0D)
                    .withFloat64("totalBaseline", measureResult.baselineTotal())
                    .withFloat64("totalTarget", measureResult.targetTotal())
                    .withInt64("leafCount", 0L)
                    .withBool("noFinding", true)
                    .withEventTime(timestamp)
                    .build();
        }
    }

    /** ISO-8601 duration or day/week-based period (calendar-ambiguous units like P1M are rejected). */
    private static Duration parseOffset(final String offset) {
        try {
            return Duration.parse(offset);
        } catch (final Exception e) {
            // fall through to Period
        }
        try {
            final Period period = Period.parse(offset);
            if(period.getMonths() != 0 || period.getYears() != 0) {
                throw new IllegalArgumentException("calendar-based units (months/years) are not supported: " + offset);
            }
            return Duration.ofDays(period.getDays());
        } catch (final IllegalArgumentException e) {
            throw e;
        } catch (final Exception e) {
            throw new IllegalArgumentException("failed to parse as ISO-8601 duration: " + offset);
        }
    }

    private static Schema createOutputSchema() {
        return Schema.builder()
                .withField("measure", Schema.FieldType.STRING)
                .withField("quantile", Schema.FieldType.FLOAT64.withNullable(true))
                .withField("algorithm", Schema.FieldType.STRING)
                .withField("epBasis", Schema.FieldType.STRING)
                .withField("rank", Schema.FieldType.INT64)
                .withField("elements", Schema.FieldType.array(
                        Schema.FieldType.element(Schema.builder()
                                .withField("dimension", Schema.FieldType.STRING)
                                .withField("value", Schema.FieldType.STRING)
                                .build())))
                .withField("layer", Schema.FieldType.INT64)
                .withField("riskScore", Schema.FieldType.FLOAT64.withNullable(true))
                .withField("explanatoryPower", Schema.FieldType.FLOAT64)
                .withField("surprise", Schema.FieldType.FLOAT64.withNullable(true))
                .withField("baseline", Schema.FieldType.FLOAT64)
                .withField("target", Schema.FieldType.FLOAT64)
                .withField("delta", Schema.FieldType.FLOAT64)
                .withField("deltaRatio", Schema.FieldType.FLOAT64.withNullable(true))
                .withField("totalBaseline", Schema.FieldType.FLOAT64)
                .withField("totalTarget", Schema.FieldType.FLOAT64)
                .withField("leafCount", Schema.FieldType.INT64)
                .withField("noFinding", Schema.FieldType.BOOLEAN)
                .build();
    }
}
