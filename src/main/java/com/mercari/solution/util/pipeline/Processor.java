package com.mercari.solution.util.pipeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Processor implements Serializable {

    public static class Parameters {

        protected JsonElement filter;
        protected JsonArray select;
        protected String flatten;

        public List<String> validate() {
            return new ArrayList<>();
        }

        public Parameters setDefaults() {
            return this;
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = new JsonObject();
            if(filter != null) {
                jsonObject.add("filter", filter);
            }
            if(select != null) {
                jsonObject.add("select", select);
            }
            if(flatten != null) {
                jsonObject.addProperty("flatten", flatten);
            }
            return jsonObject;
        }

        public Processor create(Schema inputSchema) {
            final Filter filter;
            if(this.filter != null && !this.filter.isJsonNull()) {
                filter = Filter.of(this.filter);
            } else {
                filter = null;
            }

            final Select select;
            if(this.select != null && this.select.isJsonArray()) {
                select = Select.of(this.select.getAsJsonArray(), inputSchema.getFields());
            } else {
                select = null;
            }

            final Unnest unnest;
            if(this.flatten != null) {
                unnest = Unnest.of(this.flatten);
            } else {
                unnest = null;
            }

            return new Processor(filter, select, unnest);
        }
    }

    protected final Filter filter;
    protected final Select select;
    protected final Unnest unnest;

    public Processor(Filter filter, Select select, Unnest unnest) {
        this.filter = filter;
        this.select = select;
        this.unnest = unnest;
    }

    public Processor(Processor processor) {
        this(processor.filter, processor.select, processor.unnest);
    }

    public Processor setup(final Schema inputSchema) {
        if(filter != null) {
            filter.setup();
        }
        if(select != null && select.useSelect()) {
            select.setup();
        }
        if(unnest != null && unnest.useUnnest()) {
            unnest.setup();
        }
        return this;
    }

    public List<Map<String, Object>> process(
            final Map<String, Object> primitiveValues,
            final Instant timestamp) {

        if(filter != null && !filter.filter(primitiveValues)) {
            return new ArrayList<>();
        }

        final Map<String, Object> values;
        if(select != null && select.useSelect()) {
            values = select.select(primitiveValues, timestamp);
        } else {
            values = primitiveValues;
        }

        if(unnest != null &&  unnest.useUnnest()) {
            return unnest.unnest(values);
        } else {
            final List<Map<String,Object>> list = new ArrayList<>();
            list.add(values);
            return list;
        }
    }

    public List<Map<String, Object>> process(MElement element) {
        if(filter != null && !filter.filter(element)) {
            return new ArrayList<>();
        }

        final Map<String, Object> values;
        if(select != null && select.useSelect()) {
            values = select.select(element, element.getTimestamp());
        } else {
            values = element.asPrimitiveMap();
        }

        if(unnest != null &&  unnest.useUnnest()) {
            return unnest.unnest(values);
        } else {
            final List<Map<String,Object>> list = new ArrayList<>();
            list.add(values);
            return list;
        }
    }

}
