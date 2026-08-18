package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Strategy;

import java.util.ArrayList;
import java.util.List;

public class TransformConfig extends ModuleConfig {

    private List<String> inputs;
    private JsonObject schema;
    private Strategy strategy;

    public List<String> getInputs() {
        if(inputs != null && !inputs.isEmpty()) {
            return inputs;
        } else if(com.mercari.solution.module.action.Actions.isActionModule(getModule())) {
            // action modules may be gated by waits alone (a waits-only mid-flow action is valid)
            return new ArrayList<>();
        } else if(inputs != null) {
            return inputs;
        } else {
            throw new IllegalArgumentException("Transform module: " + getName() + " has no input");
        }
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public Schema getSchema() {
        return Schema.parse(schema);
    }

    public Strategy getStrategy() {
        return strategy;
    }

}
