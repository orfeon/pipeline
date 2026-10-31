package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.text.Normalizer;
import java.util.*;

/**
 * Greedy CTC decoding of the best class of each step: consecutive repeats and the blank are removed, and the classes left
 * are turned to text with a vocabulary. {@code conf} is the mean probability of the classes left.
 *
 * <p>The vocabulary is {@code tokens} or a JSON file in {@code path}: an array of tokens, or {@code {"blank": 0, "chars": [...]}},
 * under {@code key} if given, where a string is parsed as JSON in turn. The tokens do not include the blank. The file is read
 * when the pipeline is built: a wrong path or key is a configuration error, rather than a failure of every worker.
 *
 * <pre>{@code
 * { "name": "read", "func": "ctc_decode", "field": "ids", "confField": "conf", "lengthField": "lengths",
 *   "path": "gs://bucket/model/meta.json", "key": "vocab", "normalize": "NFC", "minConf": 0.5 }
 * }</pre>
 */
public class CtcDecode implements SelectFunction {

    private final String name;
    private final String field;
    private final String confField;
    private final String lengthField;
    private final List<String> tokens;
    private final int blank;
    private final Normalizer.Form normalize;
    private final double minConf;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private record Vocabulary(List<String> tokens, int blank) {}

    private CtcDecode(
            final String name,
            final String field,
            final String confField,
            final String lengthField,
            final Vocabulary vocabulary,
            final Normalizer.Form normalize,
            final double minConf,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.field = field;
        this.confField = confField;
        this.lengthField = lengthField;
        this.tokens = vocabulary.tokens;
        this.blank = vocabulary.blank;
        this.normalize = normalize;
        this.minConf = minConf;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static CtcDecode of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode requires field parameter");
        }
        if(jsonObject.has("path") == jsonObject.has("tokens")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode requires either path or tokens parameter");
        }
        final String field = jsonObject.get("field").getAsString();
        requireArray(name, field, inputFields);
        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        ArrayFunctions.putParent(parents, field, inputFields);

        final String confField = jsonObject.has("confField") ? jsonObject.get("confField").getAsString() : null;
        if(confField != null) {
            requireArray(name, confField, inputFields);
            ArrayFunctions.putParent(parents, confField, inputFields);
        }
        final String lengthField = jsonObject.has("lengthField") ? jsonObject.get("lengthField").getAsString() : null;
        if(lengthField != null) {
            final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(lengthField, inputFields);
            if(!Vectors.isNumeric(type)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode lengthField: " + lengthField + " must be a number, but got: " + type);
            }
            ArrayFunctions.putParent(parents, lengthField, inputFields);
        }
        final double minConf = jsonObject.has("minConf") ? jsonObject.get("minConf").getAsDouble() : 0d;
        if(minConf > 0 && confField == null) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode minConf requires confField");
        }
        final Normalizer.Form normalize;
        try {
            normalize = jsonObject.has("normalize") ? Normalizer.Form.valueOf(jsonObject.get("normalize").getAsString()) : null;
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode normalize must be one of " + Arrays.toString(Normalizer.Form.values()) + ": " + jsonObject.get("normalize"));
        }
        final Integer blank = jsonObject.has("blank") ? Integer.valueOf(jsonObject.get("blank").getAsInt()) : null;
        final Vocabulary vocabulary = jsonObject.has("tokens")
                ? new Vocabulary(tokens(name, jsonObject.get("tokens")), blank == null ? 0 : blank)
                : readVocabulary(name, jsonObject.get("path").getAsString(), jsonObject.has("key") ? jsonObject.get("key").getAsString() : null, blank);
        if(vocabulary.blank < 0) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode blank must not be negative: " + vocabulary.blank);
        }
        final Schema.FieldType outputType = Schema.FieldType.element(List.of(
                Schema.Field.of("text", Schema.FieldType.STRING),
                Schema.Field.of("conf", Schema.FieldType.FLOAT32)));
        return new CtcDecode(name, field, confField, lengthField, vocabulary, normalize, minConf, new ArrayList<>(parents.values()), outputType, ignore);
    }

    private static void requireArray(final String name, final String field, final List<Schema.Field> inputFields) {
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        final Schema.FieldType valueType = switch (type.getType()) {
            case array -> type.getArrayValueType();
            case matrix -> type.getMatrixValueType();
            default -> null;
        };
        if(valueType == null) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode field: " + field + " must be an array, but got: " + type);
        }
        if(!Vectors.isNumeric(valueType)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode field: " + field + " must be an array of numbers, but got: " + type);
        }
    }

    private static List<String> tokens(final String name, final JsonElement element) {
        if(!element.isJsonArray() || element.getAsJsonArray().isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode tokens must be an array of strings: " + element);
        }
        final JsonArray array = element.getAsJsonArray();
        final List<String> tokens = new ArrayList<>(array.size());
        for(final JsonElement token : array) {
            tokens.add(token.getAsString());
        }
        return tokens;
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
    public boolean acceptsTensor() {
        return true;
    }

    @Override
    public void setup() {

    }

    private static Vocabulary readVocabulary(final String name, final String path, final String key, final Integer configBlank) {
        int blank = configBlank == null ? 0 : configBlank;
        JsonElement element = JsonParser.parseString(ResourceUtil.readString(path));
        if(key != null) {
            if(!element.isJsonObject() || !element.getAsJsonObject().has(key)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode key: " + key + " is not in " + path);
            }
            element = element.getAsJsonObject().get(key);
            if(element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
                element = JsonParser.parseString(element.getAsString());
            }
        }
        if(element.isJsonObject()) {
            final JsonObject vocabulary = element.getAsJsonObject();
            if(!vocabulary.has("chars")) {
                throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode vocabulary of " + path + " must have chars");
            }
            if(configBlank == null && vocabulary.has("blank")) {
                blank = vocabulary.get("blank").getAsInt();
            }
            element = vocabulary.get("chars");
        }
        return new Vocabulary(tokens(name, element), blank);
    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        final Object ids = ElementSchemaUtil.getValue(input, field);
        if(ids == null) {
            return null;
        }
        final List<?> idList = list(field, ids);
        final List<?> confList = confField == null ? null : list(confField, ElementSchemaUtil.getValue(input, confField));
        final int length = lengthField == null ? idList.size() : length(idList.size(), ElementSchemaUtil.getValue(input, lengthField));

        final List<Integer> kept = new ArrayList<>();
        double sum = 0;
        int previous = -1;
        for(int step=0; step<length; step++) {
            final int id = ((Number) idList.get(step)).intValue();
            if(id != blank && id != previous) {
                kept.add(id);
                if(confList != null) {
                    sum += ((Number) confList.get(step)).doubleValue();
                }
            }
            previous = id;
        }
        final double conf = kept.isEmpty() || confList == null ? 0d : sum / kept.size();
        final Map<String, Object> output = new HashMap<>();
        output.put("text", conf < minConf ? "" : text(kept));
        output.put("conf", (float) conf);
        return output;
    }

    private String text(final List<Integer> ids) {
        final StringBuilder text = new StringBuilder();
        for(int i=0; i<ids.size(); i++) {
            final String token = token(ids.get(i));
            text.append(token);
            // a LaTeX command must not read on into the next letters: \alpha then x is not \alphax
            if(isCommand(token) && i + 1 < ids.size() && startsWithLetter(token(ids.get(i + 1)))) {
                text.append(' ');
            }
        }
        return normalize == null ? text.toString() : Normalizer.normalize(text, normalize);
    }

    private String token(final int id) {
        final int index = id < blank ? id : id - 1;
        if(index < 0 || index >= tokens.size()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode id: " + id + " is out of a vocabulary of " + tokens.size() + " tokens");
        }
        return tokens.get(index);
    }

    private static boolean isCommand(final String token) {
        return token.length() > 2 && token.charAt(0) == '\\' && token.substring(1).chars().allMatch(Character::isLetter);
    }

    private static boolean startsWithLetter(final String token) {
        return !token.isEmpty() && Character.isLetter(token.codePointAt(0));
    }

    private int length(final int size, final Object value) {
        if(!(value instanceof Number number)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode lengthField: " + lengthField + " must be a number, but got: " + value);
        }
        return Math.max(0, Math.min(size, number.intValue()));
    }

    private List<?> list(final String fieldName, final Object value) {
        return switch (value) {
            case Tensor tensor -> tensor.asList();
            case List<?> list -> list;
            case null, default -> throw new IllegalArgumentException("SelectField: " + name + " func: ctc_decode field: " + fieldName + " must be an array, but got: " + value);
        };
    }

}
