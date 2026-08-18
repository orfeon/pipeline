package com.mercari.solution.module.transform;

import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Transform;
import com.mercari.solution.module.action.Actions;

/**
 * Transform-position adapter for the {@code action.<service>} modules (mid-flow action steps:
 * gated by inputs and/or waits, with the result envelope consumed downstream).
 * Unlike data transforms, inputs are optional — a waits-only action is valid here.
 * All behavior lives in {@link Actions} and is identical across placements.
 */
@Transform.Module(name="action")
public class ActionTransform extends Transform {

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        return Actions.expand(this, inputs.getPipeline(), inputs, errorHandler);
    }

}
