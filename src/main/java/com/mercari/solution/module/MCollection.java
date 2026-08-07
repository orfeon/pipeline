package com.mercari.solution.module;

import com.google.gson.JsonObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.util.Map;

public class MCollection extends PValueBase implements PValue {

    private final String name;
    private final Schema schema;
    private final PCollection<MElement> collection;
    // pipeline-assembly-time metadata (e.g. source table name), usable before execution starts
    private final Map<String, String> attributes;

    private MCollection(String name, PCollection<MElement> collection, Schema schema, Map<String, String> attributes) {
        this.name = name;
        this.collection = collection;//.setCoder(ElementCoder.of(schema));
        this.schema = schema;
        this.attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public static MCollection of(String name, PCollection<MElement> collection, Schema schema) {
        return new MCollection(name, collection, schema, null);
    }

    public static MCollection of(String name, PCollection<MElement> collection, Schema schema, Map<String, String> attributes) {
        return new MCollection(name, collection, schema, attributes);
    }

    @Override
    public Pipeline getPipeline() {
        return collection.getPipeline();
    }

    @Override
    public String getName() {
        return name;
    }

    public PCollection<MElement> getCollection() {
        return collection;
    }

    public Schema getSchema() {
        return schema;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return collection.expand();
    }

    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<MElement>, OutputT> t) {
        return Pipeline.applyTransform(collection, t);
    }

    public <OutputT extends POutput> OutputT apply(
            String name, PTransform<? super PCollection<MElement>, OutputT> t) {
        return Pipeline.applyTransform(name, collection, t);
    }

    public JsonObject toJsonObject() {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", name);
        if(schema != null) {
            final JsonObject schemaJson = schema.toJsonObject();
            jsonObject.add("schema", schemaJson);
        }
        return jsonObject;
    }

    /*
    public <OutputT extends POutput> OutputT apply(
            String name, String a, PTransform<MCollection, OutputT> t) {
        return Pipeline.applyTransform(name, this, t);
    }
     */

    /** Returns the {@link WindowingStrategy} of this {@link PCollection}. */
    public WindowingStrategy<?, ?> getWindowingStrategy() {
        return collection.getWindowingStrategy();
    }

    public PCollection.IsBounded isBounded() {
        return collection.isBounded();
    }

    @Override
    public void finishSpecifying(PInput input, PTransform<?, ?> transform) {
        collection.finishSpecifying(input, transform);
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        collection.finishSpecifyingOutput(transformName, input, transform);
    }

}
