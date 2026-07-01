package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.Logging;

import java.io.Serializable;
import java.util.*;

public class ModuleConfig implements Serializable {

    private String name;
    private String module;
    private JsonObject parameters;

    private Set<String> tags;
    private Set<String> waits;
    private List<String> sideInputs;
    private List<Logging> logs;

    private Boolean ignore;
    private Boolean failFast;
    private Boolean outputFailure;
    private List<FailureConfig> failureSinks;

    private DataType outputType;

    private String description;

    private Map<String, String> args;


    public String getName() {
        return name;
    }

    public String getModule() {
        return module;
    }

    public JsonObject getParameters() {
        return parameters;
    }

    public Set<String> getTags() {
        return tags;
    }

    public Set<String> getWaits() {
        if(waits == null) {
            return new HashSet<>();
        }
        return waits;
    }

    public List<String> getSideInputs() {
        if(sideInputs == null) {
            return new ArrayList<>();
        }
        return sideInputs;
    }

    public List<Logging> getLoggings() {
        return logs;
    }

    public Boolean getIgnore() {
        return ignore;
    }

    public Boolean getFailFast() {
        return failFast;
    }

    public void setFailFast(Boolean failFast) {
        if(this.failFast == null && failFast != null) {
            this.failFast = failFast;
        }
    }

    public Boolean getOutputFailure() {
        return outputFailure;
    }

    public DataType getOutputType() {
        return outputType;
    }

    public String getDescription() {
        return description;
    }

    public void applyContext(final String context) {
        if(context == null || context.isEmpty()) {
            return;
        }
        if(this.tags == null || this.tags.isEmpty()) {
            this.ignore = true;
        } else {
            this.ignore = !tags.contains(context);
        }
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

    public List<FailureConfig> getFailureSinks() {
        return failureSinks;
    }

    public void addFailureSinks(final List<FailureConfig> failureSinks) {
        if(this.failureSinks == null) {
            this.failureSinks = new ArrayList<>();
        }
        this.failureSinks.addAll(failureSinks);
    }

}
