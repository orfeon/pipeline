package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.text.tokenizer.MHuggingFaceTokenizer;
import com.mercari.solution.util.domain.text.tokenizer.MTokenizer;
import com.mercari.solution.util.domain.text.tokenizer.MTokenizerConfig;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.util.*;

public class Tokenize implements SelectFunction {

    private final String name;

    private final Mode mode;
    private final MTokenizer tokenizer;
    private final String field;
    private final String path;
    private final boolean skipSpecialTokens;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    public enum Mode {
        encode,
        decode
    }

    Tokenize(
            String name,
            Mode mode,
            MTokenizer.TokenizerName tokenizerName,
            String field,
            String path,
            MTokenizerConfig config,
            boolean skipSpecialTokens,
            boolean ignore) {

        this.name = name;

        this.mode = mode;
        this.tokenizer = switch (tokenizerName) {
            case huggingface -> MHuggingFaceTokenizer.create(path, field, config);
            default -> throw new IllegalArgumentException();
        };
        this.field = field;
        this.path = path;
        this.skipSpecialTokens = skipSpecialTokens;

        this.inputFields = new ArrayList<>();
        switch (mode) {
            case encode -> {
                this.inputFields.add(Schema.Field.of(field, Schema.FieldType.STRING));
                this.outputFieldType = Schema.FieldType.element(this.tokenizer.getOutputFields());
            }
            case decode -> {
                this.inputFields.add(Schema.Field.of(field, Schema.FieldType.array(Schema.FieldType.INT64)));
                this.outputFieldType = Schema.FieldType.STRING;
            }
            default -> throw new IllegalArgumentException("Not supported tokenize mode: " + mode);
        }
        this.ignore = ignore;
    }

    public static Tokenize of(
            final String name,
            final JsonObject jsonObject,
            final List<Schema.Field> inputFields,
            final boolean encode,
            final boolean ignore) {

        final Mode mode = encode ? Mode.encode : Mode.decode;

        final MTokenizer.TokenizerName tokenizer;
        if(!jsonObject.has("tokenizer")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires tokenizer parameter");
        }
        tokenizer = MTokenizer.TokenizerName.valueOf(jsonObject.get("tokenizer").getAsString().strip());

        final String field;
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires field parameter");
        }
        field = jsonObject.get("field").getAsString();

        final String path;
        if(!jsonObject.has("path")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires path parameter");
        }
        path = jsonObject.get("path").getAsString();

        final MTokenizerConfig config = new Gson().fromJson(jsonObject, MTokenizerConfig.class);

        final boolean skipSpecialTokens = jsonObject.has("skipSpecialTokens")
                && jsonObject.get("skipSpecialTokens").getAsBoolean();

        return new Tokenize(name, mode, tokenizer, field, path, config, skipSpecialTokens, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {
        this.tokenizer.setup();
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        return switch (mode) {
            case encode -> Optional
                        .ofNullable(ElementSchemaUtil.getValue(input, field))
                        .map(Object::toString)
                        .map(tokenizer::encode)
                        .orElse(null);
            case decode -> {
                final Object value = ElementSchemaUtil.getValue(input, field);
                if(value == null) {
                    yield null;
                }
                if(!(value instanceof List<?> list)) {
                    throw new IllegalArgumentException("SelectField: " + name + " tokenize_decode field: " + field + " must be an array of token ids, but: " + value);
                }
                final long[] ids = new long[list.size()];
                for(int i=0; i<ids.length; i++) {
                    ids[i] = ((Number) list.get(i)).longValue();
                }
                yield tokenizer.decode(ids, skipSpecialTokens);
            }
            default -> throw new IllegalArgumentException();
        };
    }

}

