package com.mercari.solution.module.transform;

import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.attribution.*;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

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

    private enum MeasureType { fundamental, derived, distribution, sketch }
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
            private List<Double> quantiles; // reserved for type: distribution
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
        // requires mode: series), 5 (expressiveness above slice requires fdrControl) and
        // 6 (distribution measures cannot use shapley): re-instate those checks verbatim when
        // the corresponding values are unlocked.
        private void validateReserved(final String prefix, final List<String> errorMessages) {
            if(measures != null) {
                for(final MeasureParameter measure : measures) {
                    if(MeasureType.distribution.equals(measure.type) || MeasureType.sketch.equals(measure.type)) {
                        errorMessages.add(prefix + "measures.type: " + measure.type + " is reserved and not implemented yet");
                    }
                }
            }
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

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        // Small execution profile: collect the (already aggregated) leaves into a single group
        // and run the attribution core in one worker
        final PCollectionTuple outputs = inputs
                .apply("Union", Union.withKeys(List.of())
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()))
                .apply("GroupAll", GroupByKey.create())
                .apply("Attribute", ParDo
                        .of(new AttributionDoFn(task, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        if(errorHandler != null) {
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
            final List<MeasureSpec> measures = new ArrayList<>();
            for(final Parameters.MeasureParameter measure : parameters.measures) {
                if(MeasureType.derived.equals(measure.type)) {
                    final List<String> variables = new ArrayList<>(
                            new TreeSet<>(ExpressionUtil.estimateVariables(measure.expression)));
                    measures.add(MeasureSpec.derived(measure.name, measure.expression, variables));
                    columnNames.addAll(variables);
                } else {
                    measures.add(MeasureSpec.fundamental(measure.name));
                    columnNames.add(measure.name);
                }
            }
            task.measures = measures;
            task.columnNames = new ArrayList<>(columnNames);

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
    }

    private enum Role { TARGET, BASELINE, DROP }

    private static class AttributionDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {

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
                final Iterable<MElement> elements = c.element().getValue();

                // timeShift anchors its two comparison windows at the max time in the data,
                // so runs are deterministic and reproducible with no extra configuration
                Long maxEpochMillis = null;
                if(ReferenceStrategy.timeShift.equals(task.strategy)) {
                    for(final MElement element : elements) {
                        final Long epochMillis = extractEpochMillis(element);
                        if(epochMillis != null && (maxEpochMillis == null || epochMillis > maxEpochMillis)) {
                            maxEpochMillis = epochMillis;
                        }
                    }
                    if(maxEpochMillis == null) {
                        return;
                    }
                }

                final List<String> dimensionNames = DimensionSpec.names(task.dimensions);
                final LeafTable.Builder builder = LeafTable.builder(dimensionNames, task.columnNames);
                long dropped = 0;
                for(final MElement element : elements) {
                    Logging.log(LOG, logs, "input", element);
                    final Role role = resolve(element, maxEpochMillis);
                    if(Role.DROP.equals(role)) {
                        dropped++;
                        continue;
                    }
                    final String[] dims = new String[dimensionNames.size()];
                    for(int i = 0; i < dims.length; i++) {
                        dims[i] = element.getAsString(dimensionNames.get(i));
                    }
                    final double[] values = new double[task.columnNames.size()];
                    for(int i = 0; i < values.length; i++) {
                        final Double value = element.getAsDouble(task.columnNames.get(i));
                        values[i] = value == null ? 0.0 : value;
                    }
                    if(Role.TARGET.equals(role)) {
                        builder.addTarget(dims, values);
                    } else {
                        builder.addBaseline(dims, values);
                    }
                }
                if(dropped > 0) {
                    LOG.info("attribution dropped {} rows not matching the {} reference", dropped, task.strategy);
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
                    final Long epochMillis = extractEpochMillis(element);
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

        private Long extractEpochMillis(final MElement element) {
            if(task.timeField == null) {
                return element.getEpochMillis();
            }
            final org.joda.time.Instant instant = element.getAsJodaInstant(task.timeField);
            return instant == null ? null : instant.getMillis();
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

            return MElement.builder()
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
