package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Regular expression select functions over a string field ({@link java.util.regex.Pattern} syntax). A null input gives
 * null. {@code regexp_extract} gives the first match (null without one), {@code regexp_extract_all} every match in
 * order (an empty array without one), and {@code group} picks a capturing group, by number or by name, in place of the
 * whole match — a group that takes no part in a match gives null. {@code regexp_replace} replaces every match, with
 * {@code $n} / {@code ${name}} in the replacement for a group. The pattern and the group references are checked when
 * the config is read.
 *
 * <pre>{@code
 * { "name": "groups", "func": "regexp_extract_all", "field": "positions", "pattern": "\\([^)]*\\)|[0-9]+" },
 * { "name": "code",   "func": "regexp_extract",     "field": "url", "pattern": "/items/([A-Z0-9]+)", "group": 1 },
 * { "name": "digits", "func": "regexp_replace",     "field": "phone", "pattern": "[^0-9]", "replacement": "" }
 * }</pre>
 */
public class Regexp implements SelectFunction {

    public enum Op implements Serializable {
        regexp_extract,
        regexp_extract_all,
        regexp_replace
    }

    private final String name;
    private final Op op;
    private final String field;
    private final String patternText;
    private final Integer groupIndex;
    private final String groupName;
    private final String replacement;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Pattern pattern;

    private Regexp(
            final String name,
            final Op op,
            final String field,
            final String patternText,
            final Integer groupIndex,
            final String groupName,
            final String replacement,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.op = op;
        this.field = field;
        this.patternText = patternText;
        this.groupIndex = groupIndex;
        this.groupName = groupName;
        this.replacement = replacement;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Regexp of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final Op op, final boolean ignore) {
        final String prefix = "SelectField: " + name + " func: " + op;
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException(prefix + " requires field parameter");
        }
        if(!jsonObject.has("pattern") || !jsonObject.get("pattern").isJsonPrimitive()) {
            throw new IllegalArgumentException(prefix + " requires pattern parameter");
        }
        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(type == null || type.getType() != Schema.Type.string) {
            throw new IllegalArgumentException(prefix + " field: " + field + " must be a string (cast it first), but got: " + type);
        }
        final String patternText = jsonObject.get("pattern").getAsString();
        final Pattern pattern;
        try {
            pattern = Pattern.compile(patternText);
        } catch (final PatternSyntaxException e) {
            throw new IllegalArgumentException(prefix + " pattern: " + patternText + " is not a regular expression: " + e.getDescription(), e);
        }

        Integer groupIndex = null;
        String groupName = null;
        String replacement = null;
        if(op == Op.regexp_replace) {
            if(jsonObject.has("group")) {
                throw new IllegalArgumentException(prefix + " does not take group: refer to a group in the replacement as $n or ${name}");
            }
            if(!jsonObject.has("replacement") || !jsonObject.get("replacement").isJsonPrimitive()) {
                throw new IllegalArgumentException(prefix + " requires replacement parameter");
            }
            replacement = jsonObject.get("replacement").getAsString();
            checkReplacement(prefix, pattern, replacement);
        } else if(jsonObject.has("group")) {
            final JsonElement group = jsonObject.get("group");
            if(!group.isJsonPrimitive()) {
                throw new IllegalArgumentException(prefix + " group must be a number or a name: " + group);
            }
            if(group.getAsJsonPrimitive().isNumber()) {
                groupIndex = group.getAsInt();
                if(groupIndex < 0 || groupIndex > pattern.matcher("").groupCount()) {
                    throw new IllegalArgumentException(prefix + " group: " + groupIndex + " is not a group of pattern: " + patternText
                            + " (it has " + pattern.matcher("").groupCount() + ")");
                }
            } else {
                groupName = group.getAsString();
                if(!pattern.namedGroups().containsKey(groupName)) {
                    throw new IllegalArgumentException(prefix + " group: " + groupName + " is not a named group of pattern: " + patternText
                            + " (it has " + pattern.namedGroups().keySet() + ")");
                }
            }
        }

        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        ArrayFunctions.putParent(parents, field, inputFields);
        final Schema.FieldType outputFieldType = op == Op.regexp_extract_all
                ? Schema.FieldType.array(Schema.FieldType.STRING)
                : Schema.FieldType.STRING;
        return new Regexp(name, op, field, patternText, groupIndex, groupName, replacement, new ArrayList<>(parents.values()), outputFieldType, ignore);
    }

    // Matcher.replaceAll fails on a group the pattern lacks only when something matches: fail the config instead
    private static void checkReplacement(final String prefix, final Pattern pattern, final String replacement) {
        final int groups = pattern.matcher("").groupCount();
        for(int i = 0; i < replacement.length(); i++) {
            final char c = replacement.charAt(i);
            if(c == '\\') {
                if(i + 1 == replacement.length()) {
                    throw new IllegalArgumentException(prefix + " replacement: " + replacement + " ends with an escape character");
                }
                i++;
            } else if(c == '$') {
                if(i + 1 == replacement.length()) {
                    throw new IllegalArgumentException(prefix + " replacement: " + replacement + " ends with $ (write \\$ for a dollar sign)");
                }
                final char next = replacement.charAt(i + 1);
                if(next == '{') {
                    final int end = replacement.indexOf('}', i + 2);
                    if(end < 0) {
                        throw new IllegalArgumentException(prefix + " replacement: " + replacement + " has an unclosed ${");
                    }
                    final String groupName = replacement.substring(i + 2, end);
                    if(!pattern.namedGroups().containsKey(groupName)) {
                        throw new IllegalArgumentException(prefix + " replacement: " + replacement + " refers to ${" + groupName
                                + "}, which is not a named group of the pattern (it has " + pattern.namedGroups().keySet() + ")");
                    }
                    i = end;
                } else if(Character.isDigit(next)) {
                    // as Matcher reads it: the first digit, then more while the number stays a group
                    int group = next - '0';
                    if(group > groups) {
                        throw new IllegalArgumentException(prefix + " replacement: " + replacement + " refers to $" + group
                                + ", but the pattern has " + groups + " group(s) (write \\$ for a dollar sign)");
                    }
                    i++;
                    while(i + 1 < replacement.length() && Character.isDigit(replacement.charAt(i + 1))) {
                        final int longer = group * 10 + (replacement.charAt(i + 1) - '0');
                        if(longer > groups) {
                            break;
                        }
                        group = longer;
                        i++;
                    }
                } else {
                    throw new IllegalArgumentException(prefix + " replacement: " + replacement + " has $ before neither a group number nor ${name}"
                            + " (write \\$ for a dollar sign)");
                }
            }
        }
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
        this.pattern = Pattern.compile(patternText);
    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        final Object value = ElementSchemaUtil.getValue(input, field);
        if(value == null) {
            return null;
        }
        final Matcher matcher = pattern.matcher(value.toString());
        return switch (op) {
            case regexp_extract -> matcher.find() ? group(matcher) : null;
            case regexp_extract_all -> {
                final List<String> values = new ArrayList<>();
                while(matcher.find()) {
                    values.add(group(matcher));
                }
                yield values;
            }
            case regexp_replace -> matcher.replaceAll(replacement);
        };
    }

    private String group(final Matcher matcher) {
        if(groupName != null) {
            return matcher.group(groupName);
        }
        return matcher.group(groupIndex == null ? 0 : groupIndex);
    }

}
