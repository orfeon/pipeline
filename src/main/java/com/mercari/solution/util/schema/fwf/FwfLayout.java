package com.mercari.solution.util.schema.fwf;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.domain.file.YamlUtil;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.*;

/**
 * The fixed-width layout document (work_fixedwidth.md §4.2 / §4.3): which byte (or character)
 * range of a record maps to which named, typed field. Supplied through {@code schema.reference}
 * (uri or inline) together with {@code schema.encoding.format: fwf}.
 *
 * <pre>
 * { "recordLength": 1022,
 *   "fields": [
 *     { "name": "key", "pos": 1, "fields": [ { "name": "a", "type": "string", "len": 2 }, ... ] },
 *     { "name": "amount", "type": "decimal", "pos": 55, "len": 5, "scale": 1 },
 *     { "name": "items", "pos": 60, "repeat": 3, "fields": [ ... ] } ] }
 * </pre>
 *
 * {@code pos} is 1-based and relative to the enclosing scope (the record for top-level fields, the
 * group element for children); when omitted the field starts right after the previous one.
 * Ranges may overlap (a composite key can be read as a whole and as its parts) and gaps are
 * skipped, so reserved bytes and the line separator need not be declared.
 */
public class FwfLayout implements Serializable {

    private static final Set<String> LAYOUT_KEYS = Set.of("recordLength", "fields", "description");
    private static final Set<String> FIELD_KEYS = Set.of(
            "name", "type", "mode", "description", "options",
            "pos", "len", "repeat", "size", "fields",
            "scale", "pattern", "zone", "radix", "nullIf", "trim", "defaultValue");
    private static final Map<String, String> KEY_HINTS = Map.of(
            "offset", "pos", "position", "pos", "start", "pos",
            "length", "len", "width", "len", "occurs", "repeat", "count", "repeat");

    private final Integer recordLength;
    private final String description;
    private final List<Field> fields;

    private FwfLayout(final Integer recordLength, final String description, final List<Field> fields) {
        this.recordLength = recordLength;
        this.description = description;
        this.fields = fields;
    }

    /** Data length of one record excluding the line separator; null when not declared. */
    public Integer getRecordLength() {
        return recordLength;
    }

    public String getDescription() {
        return description;
    }

    public List<Field> getFields() {
        return fields;
    }

    /** The logical output fields derived from the layout. */
    public List<Schema.Field> toSchemaFields() {
        final List<Schema.Field> schemaFields = new ArrayList<>();
        for(final Field field : fields) {
            schemaFields.add(field.toSchemaField());
        }
        return schemaFields;
    }

    /**
     * A layout restricted to the given top-level fields, in the given order (schema-redesign.md P3:
     * a narrower field list is a projection; unknown names are an error).
     */
    public FwfLayout project(final List<String> names) {
        if(names == null || names.isEmpty()) {
            return this;
        }
        final Map<String, Field> byName = new LinkedHashMap<>();
        for(final Field field : fields) {
            byName.put(field.name, field);
        }
        final List<String> missing = names.stream().filter(n -> !byName.containsKey(n)).toList();
        if(!missing.isEmpty()) {
            throw new IllegalArgumentException("fwf layout does not have fields " + missing + ". available fields: " + byName.keySet());
        }
        final List<Field> projected = names.stream().distinct().map(byName::get).toList();
        return new FwfLayout(recordLength, description, projected);
    }

    public static FwfLayout parse(final String text) {
        if(text == null || text.isBlank()) {
            throw new IllegalArgumentException("fwf layout document must not be empty");
        }
        final String trimmed = text.trim();
        final JsonElement element;
        if(trimmed.startsWith("{") || trimmed.startsWith("[")) {
            element = new Gson().fromJson(trimmed, JsonElement.class);
        } else {
            element = YamlUtil.toJson(trimmed);
        }
        return parse(element);
    }

    public static FwfLayout parse(final JsonElement element) {
        final List<String> errors = new ArrayList<>();
        final FwfLayout layout = parse(element, errors);
        if(!errors.isEmpty()) {
            throw new IllegalArgumentException("Illegal fwf layout: " + String.join(", ", errors));
        }
        return layout;
    }

    private static FwfLayout parse(final JsonElement element, final List<String> errors) {
        final JsonArray fieldsArray;
        Integer recordLength = null;
        String description = null;
        if(element == null || element.isJsonNull()) {
            errors.add("layout document must not be empty");
            return null;
        } else if(element.isJsonArray()) {
            // shorthand: the fields array alone
            fieldsArray = element.getAsJsonArray();
        } else if(element.isJsonObject()) {
            final JsonObject object = element.getAsJsonObject();
            for(final String key : object.keySet()) {
                if(!LAYOUT_KEYS.contains(key)) {
                    errors.add("layout." + key + " is not supported. supported keys: " + LAYOUT_KEYS);
                }
            }
            if(!object.has("fields") || !object.get("fields").isJsonArray()) {
                errors.add("layout.fields must be an array");
                return null;
            }
            fieldsArray = object.getAsJsonArray("fields");
            if(object.has("recordLength")) {
                recordLength = positiveInt(object.get("recordLength"), "layout.recordLength", errors);
            }
            if(object.has("description") && object.get("description").isJsonPrimitive()) {
                description = object.get("description").getAsString();
            }
        } else {
            errors.add("layout document must be an object or an array. but: " + element);
            return null;
        }

        final List<Field> fields = parseFields(fieldsArray, "fields", errors);
        if(fields.isEmpty() && errors.isEmpty()) {
            errors.add("layout.fields must not be empty");
        }
        if(recordLength != null) {
            for(final Field field : fields) {
                if(field.start + field.extent() > recordLength) {
                    errors.add(String.format("field %s (pos %d, %d units) exceeds recordLength %d",
                            field.name, field.start + 1, field.extent(), recordLength));
                }
            }
        }
        return new FwfLayout(recordLength, description, fields);
    }

    private static List<Field> parseFields(final JsonArray array, final String path, final List<String> errors) {
        final List<Field> fields = new ArrayList<>();
        final Set<String> names = new HashSet<>();
        int cursor = 0;
        int index = 0;
        for(final JsonElement element : array) {
            final String elementPath = path + "[" + index + "]";
            index++;
            if(!element.isJsonObject()) {
                errors.add(elementPath + " must be an object. but: " + element);
                continue;
            }
            final Field field = parseField(element.getAsJsonObject(), elementPath, cursor, errors);
            if(field == null) {
                continue;
            }
            if(!names.add(field.name)) {
                errors.add(elementPath + ": duplicated field name: " + field.name);
            }
            fields.add(field);
            cursor = field.start + field.extent();
        }
        return fields;
    }

    private static Field parseField(
            final JsonObject object,
            final String path,
            final int cursor,
            final List<String> errors) {

        final int errorCount = errors.size();
        for(final String key : object.keySet()) {
            if(!FIELD_KEYS.contains(key)) {
                final String hint = KEY_HINTS.get(key);
                errors.add(path + "." + key + " is not supported" + (hint != null ? " (use '" + hint + "')" : "")
                        + ". supported keys: " + FIELD_KEYS);
            }
        }
        if(!object.has("name") || !object.get("name").isJsonPrimitive() || object.get("name").getAsString().isBlank()) {
            errors.add(path + ".name is required");
            return null;
        }
        final Field field = new Field();
        field.name = object.get("name").getAsString();
        final String fieldPath = path + "(" + field.name + ")";

        final boolean group = object.has("fields");
        if(object.has("type")) {
            try {
                field.type = Schema.Type.of(object.get("type").getAsString());
            } catch (final IllegalArgumentException e) {
                errors.add(fieldPath + ".type: " + e.getMessage());
                return null;
            }
        } else {
            field.type = group ? Schema.Type.element : Schema.Type.string;
        }

        if(object.has("mode")) {
            final String mode = object.get("mode").getAsString().trim().toLowerCase(Locale.ROOT);
            switch (mode) {
                case "nullable" -> field.required = false;
                case "required" -> field.required = true;
                case "repeated" -> errors.add(fieldPath + ".mode: repeated is implied by 'repeat'; use repeat instead");
                default -> errors.add(fieldPath + ".mode must be nullable or required. but: " + mode);
            }
        }
        if(object.has("description") && object.get("description").isJsonPrimitive()) {
            field.description = object.get("description").getAsString();
        }
        if(object.has("options") && object.get("options").isJsonObject()) {
            field.options = new HashMap<>();
            for(final Map.Entry<String, JsonElement> entry : object.getAsJsonObject("options").entrySet()) {
                if(entry.getValue().isJsonPrimitive()) {
                    field.options.put(entry.getKey(), entry.getValue().getAsString());
                }
            }
        }

        if(object.has("pos")) {
            final Integer pos = positiveInt(object.get("pos"), fieldPath + ".pos", errors);
            field.start = pos == null ? cursor : pos - 1;
        } else {
            field.start = cursor;
        }
        if(object.has("repeat")) {
            field.repeat = positiveInt(object.get("repeat"), fieldPath + ".repeat", errors);
        }

        if(group) {
            if(field.type != Schema.Type.element) {
                errors.add(fieldPath + ": a field with 'fields' must be a record (omit type or set element/record). but: " + field.type);
                return null;
            }
            for(final String key : List.of("len", "scale", "pattern", "zone", "radix", "nullIf", "trim", "defaultValue")) {
                if(object.has(key)) {
                    errors.add(fieldPath + "." + key + " is not allowed on a group field (one with 'fields')");
                }
            }
            if(!object.get("fields").isJsonArray()) {
                errors.add(fieldPath + ".fields must be an array");
                return null;
            }
            field.children = parseFields(object.getAsJsonArray("fields"), fieldPath + ".fields", errors);
            if(field.children.isEmpty()) {
                errors.add(fieldPath + ".fields must not be empty");
                return null;
            }
            int span = 0;
            for(final Field child : field.children) {
                span = Math.max(span, child.start + child.extent());
            }
            if(object.has("size")) {
                final Integer size = positiveInt(object.get("size"), fieldPath + ".size", errors);
                if(size != null && size < span) {
                    errors.add(String.format("%s.size %d is smaller than the extent of its fields %d", fieldPath, size, span));
                }
                field.size = size == null ? span : size;
            } else {
                field.size = span;
            }
        } else {
            if(field.type == Schema.Type.element) {
                errors.add(fieldPath + ": a record field requires 'fields'");
                return null;
            }
            if(object.has("size")) {
                errors.add(fieldPath + ".size is only allowed on a group field (one with 'fields')");
            }
            switch (field.type) {
                case bool, string, json, bytes, int16, int32, int64, float32, float64, decimal, date, time, timestamp -> {}
                default -> {
                    errors.add(fieldPath + ".type: " + field.type + " is not supported by fwf. supported types: "
                            + "bool, string, json, bytes, int16, int32, int64, float32, float64, decimal, date, time, timestamp");
                    return null;
                }
            }
            if(!object.has("len")) {
                errors.add(fieldPath + ".len is required");
                return null;
            }
            final Integer len = positiveInt(object.get("len"), fieldPath + ".len", errors);
            field.len = len == null ? 1 : len;
            parseConversion(field, object, fieldPath, errors);
        }

        return errors.size() == errorCount ? field : null;
    }

    private static void parseConversion(
            final Field field,
            final JsonObject object,
            final String fieldPath,
            final List<String> errors) {

        if(object.has("scale")) {
            switch (field.type) {
                case decimal, float32, float64 -> {
                    final Integer scale = nonNegativeInt(object.get("scale"), fieldPath + ".scale", errors);
                    field.scale = scale;
                }
                default -> errors.add(fieldPath + ".scale is only allowed on decimal, float32, float64. but type is " + field.type);
            }
        }
        if(object.has("pattern")) {
            switch (field.type) {
                case date, time, timestamp -> {
                    field.pattern = object.get("pattern").getAsString();
                    try {
                        DateTimeFormatter.ofPattern(field.pattern, Locale.ROOT);
                    } catch (final IllegalArgumentException e) {
                        errors.add(fieldPath + ".pattern: " + field.pattern + " is illegal: " + e.getMessage());
                    }
                }
                default -> errors.add(fieldPath + ".pattern is only allowed on date, time, timestamp. but type is " + field.type);
            }
        }
        if(object.has("zone")) {
            if(field.type != Schema.Type.timestamp) {
                errors.add(fieldPath + ".zone is only allowed on timestamp. but type is " + field.type);
            } else {
                field.zone = object.get("zone").getAsString();
                try {
                    ZoneId.of(field.zone);
                } catch (final Exception e) {
                    errors.add(fieldPath + ".zone: " + field.zone + " is illegal: " + e.getMessage());
                }
            }
        }
        if(object.has("radix")) {
            switch (field.type) {
                case int16, int32, int64 -> {
                    final Integer radix = positiveInt(object.get("radix"), fieldPath + ".radix", errors);
                    if(radix != null && (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX)) {
                        errors.add(fieldPath + ".radix must be between 2 and 36. but: " + radix);
                    } else {
                        field.radix = radix;
                    }
                }
                default -> errors.add(fieldPath + ".radix is only allowed on int16, int32, int64. but type is " + field.type);
            }
        }
        if(object.has("nullIf")) {
            if(field.type == Schema.Type.bytes) {
                errors.add(fieldPath + ".nullIf is not allowed on bytes");
            } else {
                final JsonElement nullIf = object.get("nullIf");
                field.nullIf = new ArrayList<>();
                if(nullIf.isJsonArray()) {
                    for(final JsonElement e : nullIf.getAsJsonArray()) {
                        field.nullIf.add(FwfDecoder.strip(e.getAsString(), FwfOptions.Trim.both));
                    }
                } else if(nullIf.isJsonPrimitive()) {
                    field.nullIf.add(FwfDecoder.strip(nullIf.getAsString(), FwfOptions.Trim.both));
                } else {
                    errors.add(fieldPath + ".nullIf must be a string or an array of strings");
                }
            }
        }
        if(object.has("trim")) {
            try {
                field.trim = FwfOptions.Trim.valueOf(object.get("trim").getAsString().trim().toLowerCase(Locale.ROOT));
            } catch (final IllegalArgumentException e) {
                errors.add(fieldPath + ".trim must be one of both, left, right, none");
            }
        }
        if(object.has("defaultValue")) {
            if(field.type == Schema.Type.bytes) {
                errors.add(fieldPath + ".defaultValue is not allowed on bytes");
            } else if(!object.get("defaultValue").isJsonPrimitive()) {
                errors.add(fieldPath + ".defaultValue must be a primitive value");
            } else {
                final String text = object.get("defaultValue").getAsString();
                try {
                    field.defaultValue = field.type == Schema.Type.string || field.type == Schema.Type.json
                            ? text : field.convert(text.strip());
                } catch (final Exception e) {
                    errors.add(fieldPath + ".defaultValue: " + text + " can not be converted to " + field.type + ": " + e.getMessage());
                }
            }
        }
    }

    private static Integer positiveInt(final JsonElement element, final String path, final List<String> errors) {
        final Integer value = integer(element, path, errors);
        if(value != null && value < 1) {
            errors.add(path + " must be a positive integer. but: " + value);
            return null;
        }
        return value;
    }

    private static Integer nonNegativeInt(final JsonElement element, final String path, final List<String> errors) {
        final Integer value = integer(element, path, errors);
        if(value != null && value < 0) {
            errors.add(path + " must not be negative. but: " + value);
            return null;
        }
        return value;
    }

    private static Integer integer(final JsonElement element, final String path, final List<String> errors) {
        try {
            if(element.isJsonPrimitive()) {
                return Integer.valueOf(element.getAsString().trim());
            }
        } catch (final NumberFormatException e) {
            // fall through
        }
        errors.add(path + " must be an integer. but: " + element);
        return null;
    }

    /**
     * One layout field: a leaf (a typed range) or a group (a record of child fields), optionally
     * repeated ({@code repeat} consecutive elements → an array).
     */
    public static class Field implements Serializable {

        private String name;
        private Schema.Type type;
        private boolean required;
        private String description;
        private Map<String, String> options;

        // 0-based start within the enclosing scope
        private int start;
        // leaf: units per element
        private int len;
        // group: units per element
        private int size;
        private Integer repeat;
        private List<Field> children;

        private Integer scale;
        private String pattern;
        private String zone;
        private Integer radix;
        private List<String> nullIf;
        private FwfOptions.Trim trim;
        private Object defaultValue;

        private transient DateTimeFormatter formatter;

        public String getName() {
            return name;
        }

        public Schema.Type getType() {
            return type;
        }

        public boolean isGroup() {
            return children != null;
        }

        public boolean isRequired() {
            return required;
        }

        public int getStart() {
            return start;
        }

        public int getLen() {
            return len;
        }

        public int getSize() {
            return size;
        }

        public Integer getRepeat() {
            return repeat;
        }

        public List<Field> getChildren() {
            return children;
        }

        public List<String> getNullIf() {
            return nullIf;
        }

        public FwfOptions.Trim getTrim() {
            return trim;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        /** Units of one element (len for a leaf, size for a group). */
        public int unit() {
            return isGroup() ? size : len;
        }

        /** Total units occupied, repeats included. */
        public int extent() {
            return unit() * (repeat == null ? 1 : repeat);
        }

        Schema.Field toSchemaField() {
            Schema.FieldType fieldType;
            if(isGroup()) {
                final List<Schema.Field> childFields = new ArrayList<>();
                for(final Field child : children) {
                    childFields.add(child.toSchemaField());
                }
                fieldType = Schema.FieldType.element(childFields);
            } else if(type == Schema.Type.decimal) {
                fieldType = Schema.FieldType.decimal(38, 9);
            } else {
                fieldType = Schema.FieldType.type(type);
            }
            fieldType = fieldType.withNullable(!required);
            if(repeat != null) {
                fieldType = Schema.FieldType.array(fieldType);
            }
            final Schema.Field field = Schema.Field.of(name, fieldType);
            if(description != null) {
                field.withDescription(description);
            }
            if(options != null && !options.isEmpty()) {
                field.withOptions(new HashMap<>(options));
            }
            return field;
        }

        /**
         * Converts the stripped, non-empty text of a leaf into the element value representation
         * (date: epoch day Integer, time: micro of day Long, timestamp: epoch micros Long).
         */
        Object convert(final String text) {
            return switch (type) {
                case string, json -> text;
                case bool -> switch (text.toLowerCase(Locale.ROOT)) {
                    case "1", "true", "t", "y", "yes" -> true;
                    case "0", "false", "f", "n", "no" -> false;
                    default -> throw new IllegalArgumentException("not a boolean: " + text);
                };
                case int16 -> Short.parseShort(signed(text), radix == null ? 10 : radix);
                case int32 -> Integer.parseInt(signed(text), radix == null ? 10 : radix);
                case int64 -> Long.parseLong(signed(text), radix == null ? 10 : radix);
                case float32 -> decimal(text).floatValue();
                case float64 -> decimal(text).doubleValue();
                case decimal -> decimal(text);
                case date -> pattern == null
                        ? DateTimeUtil.toEpochDay(text)
                        : Long.valueOf(LocalDate.parse(text, formatter()).toEpochDay()).intValue();
                case time -> pattern == null
                        ? DateTimeUtil.toMicroOfDay(text)
                        : LocalTime.parse(text, formatter()).getLong(ChronoField.MICRO_OF_DAY);
                case timestamp -> pattern == null ? DateTimeUtil.toEpochMicroSecond(text) : timestamp(text);
                // bytes are sliced raw by the decoder and never converted from text
                default -> throw new IllegalStateException("fwf can not convert text to type: " + type);
            };
        }

        private BigDecimal decimal(final String text) {
            final String normalized = signed(text);
            final BigDecimal value = new BigDecimal(normalized);
            if(scale != null && scale > 0 && normalized.indexOf('.') < 0) {
                return value.movePointLeft(scale);
            }
            return value;
        }

        private Long timestamp(final String text) {
            final TemporalAccessor parsed = formatter().parseBest(text,
                    ZonedDateTime::from, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);
            final Instant instant = switch (parsed) {
                case ZonedDateTime z -> z.toInstant();
                case OffsetDateTime o -> o.toInstant();
                case LocalDateTime l -> l.atZone(zoneId()).toInstant();
                case LocalDate d -> d.atStartOfDay(zoneId()).toInstant();
                default -> throw new IllegalArgumentException("can not parse timestamp: " + text);
            };
            return DateTimeUtil.toEpochMicroSecond(instant);
        }

        private ZoneId zoneId() {
            return zone == null ? ZoneId.of("UTC") : ZoneId.of(zone);
        }

        private DateTimeFormatter formatter() {
            if(formatter == null) {
                formatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
            }
            return formatter;
        }

        // "+12" / "- 4" / "-1.5": a leading sign may be separated from the digits by spaces
        private static String signed(final String text) {
            if(text.length() > 1 && (text.charAt(0) == '+' || text.charAt(0) == '-')) {
                final String rest = FwfDecoder.strip(text.substring(1), FwfOptions.Trim.left);
                return text.charAt(0) == '-' ? "-" + rest : rest;
            }
            return text;
        }

    }

}
