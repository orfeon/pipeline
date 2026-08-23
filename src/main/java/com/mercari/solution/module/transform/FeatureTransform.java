package com.mercari.solution.module.transform;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.*;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.feature.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Declarative feature generation (work-feature.md): row / context / sequence / population features with
 * availability-time leak checking against a sources contract. The spec is compiled by
 * {@link FeaturePlanCompiler} at assembly time (compile errors fail the pipeline) and executed as a
 * chain of stages by {@link FeatureStages}.
 */
@Transform.Module(name = "feature")
public class FeatureTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureTransform.class);

    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs, final MErrorHandler errorHandler) {

        final JsonObject rawParameters = Config.convertConfigJson(getParametersText(), Config.Format.json);
        final FeaturePlanService.Documents documents;
        try {
            documents = FeaturePlanService.resolve(rawParameters, getTemplateArgs());
        } catch (final IllegalArgumentException e) {
            throw new IllegalModuleException(getName(), "feature", e);
        }
        if (documents.sources() == null) {
            throw new IllegalModuleException(getName(), "feature", "parameters.sources is required (URI or inline sources definition)");
        }

        final Schema inputSchema = Union.createUnionSchema(inputs);
        final FeaturePlan plan = FeaturePlanCompiler.compile(documents.sources(), documents.parameters(), inputSchema.getFields());
        final List<String> errors = new ArrayList<>(plan.getDiagnostics().getErrorMessages());
        errors.addAll(FeatureStages.engineConstraints(plan, OptionUtil.isStreaming(inputs)));
        if (!errors.isEmpty()) {
            LOG.error("feature plan for {} failed:\n{}", getName(), plan.describe());
            throw new IllegalModuleException(getName(), "feature", errors);
        }
        LOG.info("feature plan for {}:\n{}", getName(), plan.describe());

        final DataType outputType = Optional.ofNullable(getOutputType()).orElse(DataType.AVRO);
        final Schema outputSchema = FeatureStages.createOutputSchema(plan, inputSchema, outputType);

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final FeatureStages.Result result = FeatureStages.apply(input, inputSchema, plan, outputSchema, getLoggings(), getFailFast());
        if (errorHandler != null) {
            for (final PCollection<BadRecord> failures : result.failures()) {
                errorHandler.addError(failures);
            }
        }
        return MCollectionTuple.of(result.output(), outputSchema);
    }

}
