package com.mercari.solution.util.pipeline;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;

import java.util.List;

public class Input extends Processor {

    public static class Parameters extends Processor.Parameters {

        protected String name;

        public String getName() {
            return name;
        }

        public List<String> validate() {
            final List<String> errorMessages = super.validate();
            return errorMessages;
        }

        public Input.Parameters setDefaults() {
            super.setDefaults();
            return this;
        }

        public JsonObject toJson() {
            final JsonObject jsonObject = super.toJson();
            jsonObject.addProperty("name", name);
            return jsonObject;
        }

        public Input create(Schema inputSchema) {
            final Processor processor = super.create(inputSchema);
            return new Input(name, processor);
        }
    }

    Input(String name, Processor processor) {
        super(processor);
        this.name = name;
    }

    protected String name;

    public String getName() {
        return name;
    }

    public Input setup(Schema inputSchema) {
        super.setup(inputSchema);
        return this;
    }

}
