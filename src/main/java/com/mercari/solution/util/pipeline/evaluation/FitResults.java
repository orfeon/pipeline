package com.mercari.solution.util.pipeline.evaluation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The outcome of the calibration fits (design §7.1), a side input of the align step and of the finalize
 * step: per derived set the parameters that turn its base set into it (temperature: {@code [T]}; blend:
 * {@code [a, b]} or {@code [a, b, intercept]}), and the summary records describing every fit.
 */
public final class FitResults implements Serializable {

    /** derived set name → parameters (null when the fit produced none: the set is not derived) */
    public final Map<String, double[]> parameters = new LinkedHashMap<>();
    /** one record per derived set: prediction, derived, type, fitOn, the estimates, standard errors, diagnostics */
    public final List<Map<String, Object>> records = new ArrayList<>();

    public double[] parameters(final String derived) {
        return parameters.get(derived);
    }
}
