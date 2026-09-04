package com.mercari.solution.module.transform;

import com.google.gson.JsonObject;
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
import com.mercari.solution.util.pipeline.screen.ScreenReport;
import com.mercari.solution.util.pipeline.screen.ScreenSpec;
import com.mercari.solution.util.pipeline.screen.ScreenStages;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Baseline-conditioned feature screening: scores every numeric candidate column against the label with a
 * Rao score test of an offset GLM (one closed-form Combine), calibrated by placebo columns, with period
 * sign agreement, a time window and leak-suspect flags. Outputs the scoring records (default output) and one
 * {@code summary} record. Batch only.
 */
@Transform.Module(name = "screen")
public class ScreenTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(ScreenTransform.class);

    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs, final MErrorHandler errorHandler) {
        if (OptionUtil.isStreaming(inputs)) {
            throw new IllegalModuleException(getName(), "screen", "screen transform is batch only (every statistic is a global Combine)");
        }
        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final ScreenSpec spec;
        try {
            final JsonObject parameters = JsonParser.parseString(getParametersText()).getAsJsonObject();
            final ScreenSpec parsed = ScreenSpec.parse(parameters);
            ScreenSpec.Lineage lineage = ScreenSpec.Lineage.fromSchema(inputSchema);
            if (parsed.candidateManifest != null) {
                lineage = lineage.merge(ScreenSpec.Lineage.fromManifest(Config.readContent(parsed.candidateManifest)));
            }
            spec = parsed.resolve(inputSchema, lineage);
        } catch (final IllegalArgumentException | IllegalStateException e) {
            throw new IllegalModuleException(getName(), "screen", e.getMessage());
        } catch (final IOException e) {
            throw new IllegalModuleException(getName(), "screen", "failed to read candidates.manifest: " + e.getMessage());
        }
        final java.util.List<String> constraints = ScreenStages.engineConstraints(input, spec);
        if (!constraints.isEmpty()) {
            throw new IllegalModuleException(getName(), "screen", constraints);
        }
        LOG.info(ScreenReport.describe(spec));

        final ScreenStages.Outputs outputs = ScreenStages.apply(input, spec, getLoggings(), getFailFast());
        if (errorHandler != null) {
            errorHandler.addError(outputs.failures());
        }
        return MCollectionTuple
                .of(outputs.records(), ScreenReport.recordSchema())
                .and("summary", outputs.summary(), ScreenReport.summarySchema());
    }
}
