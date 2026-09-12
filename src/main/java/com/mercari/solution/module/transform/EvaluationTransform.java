package com.mercari.solution.module.transform;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Transform;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.evaluation.EvaluationReport;
import com.mercari.solution.util.pipeline.evaluation.EvaluationSpec;
import com.mercari.solution.util.pipeline.evaluation.EvaluationStages;
import com.mercari.solution.util.pipeline.feature.FeatureLineage;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Prediction verification against a baseline: matches prediction sets with the outcome on time splits with
 * a selection / report role and reports the excess log score over the baseline (with a Poisson bootstrap CI,
 * paired between prediction sets), logloss, hit@1 and Brier, per declared slice, plus calibration tables, calibration
 * fits as derived prediction sets and slice discovery. Outputs the metrics (default), {@code calibration},
 * {@code units} (the per-unit loss decomposition), {@code slices} (the discovered slices) and one {@code summary}
 * record. Batch only.
 */
@Transform.Module(name = "evaluation")
public class EvaluationTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(EvaluationTransform.class);

    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs, final MErrorHandler errorHandler) {
        if (OptionUtil.isStreaming(inputs)) {
            throw new IllegalModuleException(getName(), "evaluation", "evaluation transform is batch only (every statistic is a global Combine)");
        }
        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final EvaluationSpec spec;
        try {
            final JsonObject parameters = JsonParser.parseString(getParametersText()).getAsJsonObject();
            final EvaluationSpec parsed = EvaluationSpec.parse(parameters);
            FeatureLineage lineage = FeatureLineage.fromSchema(inputSchema);
            if (parsed.manifest != null) {
                final String manifest;
                try {
                    manifest = Config.readContent(parsed.manifest);
                } catch (final RuntimeException e) {
                    throw new IllegalModuleException(getName(), "evaluation", "failed to read manifest '" + parsed.manifest + "': " + e.getMessage());
                }
                lineage = lineage.merge(FeatureLineage.fromManifest(manifest, "manifest"));
            }
            spec = parsed.resolve(inputSchema, lineage);
        } catch (final IllegalArgumentException | IllegalStateException | JsonParseException e) {
            throw new IllegalModuleException(getName(), "evaluation", e.getMessage());
        } catch (final IOException e) {
            throw new IllegalModuleException(getName(), "evaluation", "failed to read manifest: " + e.getMessage());
        }
        final List<String> constraints = EvaluationStages.engineConstraints(input);
        if (!constraints.isEmpty()) {
            throw new IllegalModuleException(getName(), "evaluation", constraints);
        }
        LOG.info(EvaluationReport.describe(spec));

        final EvaluationStages.Outputs outputs = EvaluationStages.apply(input, spec, getLoggings(), getFailFast());
        if (errorHandler != null) {
            errorHandler.addError(outputs.failures());
        }
        return MCollectionTuple
                .of(outputs.metrics(), EvaluationReport.metricsSchema())
                .and("calibration", outputs.calibration(), EvaluationReport.calibrationSchema())
                .and("units", outputs.units(), EvaluationReport.unitsSchema())
                .and("slices", outputs.slices(), EvaluationReport.slicesSchema())
                .and("summary", outputs.summary(), EvaluationReport.summarySchema());
    }
}
