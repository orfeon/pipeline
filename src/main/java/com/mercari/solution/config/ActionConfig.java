package com.mercari.solution.config;

import com.mercari.solution.module.Action;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.Strategy;
import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Config of an {@code actions} section step. {@code module} names the action service
 * (bigquery, http, storage, tasks, vertexai, …). {@code inputs} is optional: with
 * {@code trigger: once} the step may be gated by {@code waits} alone, or by nothing at all
 * (a pipeline-start action).
 */
public class ActionConfig extends ModuleConfig {

    private List<String> inputs;
    // kept as text: Gson maps an unknown enum constant to null, which would silently become `once`
    private String trigger;
    private String operation;
    private Strategy strategy;
    private Action.Retry retry;
    private Boolean fireOnEmpty;
    // condition text (SQL-like) or JSON condition object; kept as JsonElement and translated by the module
    private JsonElement failWhen;
    private JsonElement skipWhen;

    public List<String> getInputs() {
        if(inputs == null) {
            return new ArrayList<>();
        }
        return inputs;
    }

    /** The declared trigger, or null when absent (the module defaults it to {@code once}). */
    public Action.Trigger getTrigger() {
        if(trigger == null || trigger.isBlank()) {
            return null;
        }
        try {
            return Action.Trigger.valueOf(trigger);
        } catch (final IllegalArgumentException e) {
            throw new IllegalModuleException(
                    "Illegal trigger: " + trigger + ". supported values: once, perElement, collect");
        }
    }

    public String getOperation() {
        return operation;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public Action.Retry getRetry() {
        return retry;
    }

    public Boolean getFireOnEmpty() {
        return fireOnEmpty;
    }

    public JsonElement getFailWhen() {
        return failWhen;
    }

    public JsonElement getSkipWhen() {
        return skipWhen;
    }

}
