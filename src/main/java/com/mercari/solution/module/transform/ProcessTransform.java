package com.mercari.solution.module.transform;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Transform;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.process.ProcessSpec;
import com.mercari.solution.util.pipeline.process.ProcessStages;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Process mining over an event log: groups the input rows into cases, replays every case in time order and
 * emits the directly-follows graph ({@code edges}, the default output, and {@code nodes}), the trace
 * {@code variants}, one record per case ({@code cases}), the resource {@code handovers} and the per-constraint
 * {@code conformance} summary of the declared Declare constraints. The heavy lifting lives in
 * {@link ProcessStages} (Beam wiring) and {@code CaseReplay} (pure per-case logic).
 */
@Transform.Module(name = "process")
public class ProcessTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessTransform.class);

    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs, final MErrorHandler errorHandler) {

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final ProcessSpec spec;
        try {
            final JsonObject parameters = Config.convertConfigJson(getParametersText(), Config.Format.json);
            spec = ProcessSpec.parse(parameters);
        } catch (final IllegalArgumentException | IllegalStateException | JsonParseException e) {
            throw new IllegalModuleException(getName(), "process", e.getMessage());
        }
        final List<String> errors = spec.validate(inputSchema);
        if (!errors.isEmpty()) {
            throw new IllegalModuleException(getName(), "process", errors);
        }
        if (OptionUtil.isStreaming(inputs) && input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows) {
            throw new IllegalModuleException(getName(), "process",
                    "streaming input requires a non-global window (strategy.window, e.g. type: session) so that a window closes a case");
        }
        LOG.info("process transform {}: {}", getName(), spec.describe());

        final ProcessStages.Outputs outputs = ProcessStages.apply(input, inputSchema, spec, getLoggings(), getFailFast());
        if (errorHandler != null) {
            errorHandler.addError(outputs.failures());
        }
        return MCollectionTuple
                .of(outputs.edges(), ProcessStages.edgesSchema())
                .and("nodes", outputs.nodes(), ProcessStages.nodesSchema())
                .and("variants", outputs.variants(), ProcessStages.variantsSchema())
                .and("cases", outputs.cases(), ProcessStages.casesSchema(spec))
                .and("handovers", outputs.handovers(), ProcessStages.handoversSchema())
                .and("conformance", outputs.conformance(), ProcessStages.conformanceSchema());
    }
}
