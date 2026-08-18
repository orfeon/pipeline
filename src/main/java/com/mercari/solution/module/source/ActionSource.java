package com.mercari.solution.module.source;

import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Source;
import com.mercari.solution.module.action.Actions;
import org.apache.beam.sdk.values.PBegin;

/**
 * Source-position adapter for the {@code action.<service>} modules (pipeline-start action
 * steps with no upstream — e.g. scale up an instance before other steps, which gate on it
 * via {@code waits}). Only the {@code once} trigger applies here since sources have no inputs.
 * All behavior lives in {@link Actions} and is identical across placements.
 */
@Source.Module(name="action")
public class ActionSource extends Source {

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        return Actions.expand(this, begin.getPipeline(), MCollectionTuple.empty(begin.getPipeline()), errorHandler);
    }

}
