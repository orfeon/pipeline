package com.mercari.solution.util.domain.attribution;

import java.io.Serializable;
import java.util.List;

/** Attribution results for all measures, in declaration order. */
public record AttributionResult(
        List<MeasureResult> results) implements Serializable {
}
