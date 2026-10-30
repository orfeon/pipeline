package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.text.tokenizer.MHuggingFaceTokenizer;
import com.mercari.solution.util.domain.text.tokenizer.MTokenizer;
import com.mercari.solution.util.domain.text.tokenizer.MTokenizerConfig;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.util.*;

/**
 * {@code tokenize_encode} and {@code tokenize_decode}, and {@code token_ids}: a constant of the ids of the given texts,
 * by group, keeping those that are one token. It is computed once in setup, where a group without any fails.
 */
public class Tokenize implements SelectFunction {

    private final String name;

    private final Mode mode;
    private final MTokenizer tokenizer;
    private final String field;
    private final String path;
    private final boolean skipSpecialTokens;
    private final List<List<String>> texts;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient List<List<Long>> tokenIds;


    public enum Mode {
        encode,
        decode,
        ids
    }

    Tokenize(
            String name,
            Mode mode,
            MTokenizer.TokenizerName tokenizerName,
            String field,
            String path,
            MTokenizerConfig config,
            boolean skipSpecialTokens,
            List<List<String>> texts,
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
        this.texts = texts;

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
            case ids -> this.outputFieldType = Schema.FieldType.array(Schema.FieldType.array(Schema.FieldType.INT64));
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

        return new Tokenize(name, mode, tokenizer, field, path, config, skipSpecialTokens, null, ignore);
    }

    public static Tokenize ofIds(
            final String name,
            final JsonObject jsonObject,
            final boolean ignore) {

        for(final String parameter : List.of("tokenizer", "path", "texts")) {
            if(!jsonObject.has(parameter)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: token_ids requires " + parameter + " parameter");
            }
        }
        final MTokenizer.TokenizerName tokenizer = MTokenizer.TokenizerName.valueOf(jsonObject.get("tokenizer").getAsString().strip());
        final List<List<String>> texts = new ArrayList<>();
        if(!jsonObject.get("texts").isJsonArray() || jsonObject.getAsJsonArray("texts").isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: token_ids texts must be an array of groups: " + jsonObject.get("texts"));
        }
        for(final JsonElement group : jsonObject.getAsJsonArray("texts")) {
            // a text by itself is a group of one
            final List<JsonElement> elements = group.isJsonArray() ? group.getAsJsonArray().asList() : List.of(group);
            texts.add(elements.stream().map(JsonElement::getAsString).toList());
        }

        // a token that is a part of a text has no special tokens around it
        final MTokenizerConfig config = new MTokenizerConfig();
        config.addSpecialTokens = false;
        return new Tokenize(name, Mode.ids, tokenizer, null, jsonObject.get("path").getAsString(), config, false, texts, ignore);
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

    // token_ids reads no field: it must not turn the Tensors of the values before it into lists
    @Override
    public boolean acceptsTensor() {
        return mode == Mode.ids;
    }

    @Override
    public void setup() {
        this.tokenizer.setup();
        if(mode == Mode.ids) {
            this.tokenIds = createTokenIds();
        }
    }

    private List<List<Long>> createTokenIds() {
        final List<List<Long>> groups = new ArrayList<>();
        for(final List<String> group : texts) {
            final Set<Long> ids = new LinkedHashSet<>();
            for(final String text : group) {
                final Object encoded = tokenizer.encode(text).get("ids");
                if(encoded instanceof List<?> list && list.size() == 1) {
                    ids.add(((Number) list.getFirst()).longValue());
                }
            }
            if(ids.isEmpty()) {
                throw new IllegalArgumentException("SelectField: " + name + " func: token_ids group: " + group + " has no text of one token, in tokenizer: " + path);
            }
            groups.add(new ArrayList<>(ids));
        }
        return groups;
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
            case ids -> tokenIds;
            default -> throw new IllegalArgumentException();
        };
    }

}

