package com.mercari.solution.util.schema;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ElementSchemaUtil {

    public static Schema.Field getInputField(
            final String field,
            final List<Schema.Field> inputFields) {

        for(final Schema.Field inputField : inputFields) {
            if(field.equals(inputField.getName())) {
                return inputField;
            } else if(field.contains(".")) {
                final String[] fields = field.split("\\.", 2);
                final Schema.FieldType parentFieldType = getInputFieldType(fields[0], inputFields);
                return switch (parentFieldType.getType()) {
                    case json -> Schema.Field.of(fields[fields.length-1], Schema.FieldType.type(Schema.Type.json));
                    case element -> getInputField(fields[1], parentFieldType.getElementSchema().getFields());
                    case array -> {
                        if (!Schema.Type.element.equals(parentFieldType.getArrayValueType().getType())) {
                            throw new IllegalArgumentException();
                        }
                        yield getInputField(fields[1], parentFieldType.getArrayValueType().getElementSchema().getFields());
                    }
                    default -> throw new IllegalArgumentException();
                };
            }
        }
        throw new IllegalArgumentException("Not found field: " + field + " in input fields: " + inputFields);
    }

    public static Schema.FieldType getInputFieldType(
            final String field,
            final List<Schema.Field> inputFields) {

        for(final Schema.Field inputField : inputFields) {
            if(field.equals(inputField.getName())) {
                return inputField.getFieldType();
            } else if(field.contains(".")) {
                final String[] fields = field.split("\\.", 2);
                final Schema.FieldType parentFieldType = getInputFieldType(fields[0], inputFields);
                return switch (parentFieldType.getType()) {
                    case json -> Schema.FieldType.type(Schema.Type.json);
                    case element -> getInputFieldType(fields[1], parentFieldType.getElementSchema().getFields());
                    case array -> {
                        if (!Schema.Type.element.equals(parentFieldType.getArrayValueType().getType())) {
                            throw new IllegalArgumentException();
                        }
                        yield getInputFieldType(fields[1], parentFieldType.getArrayValueType().getElementSchema().getFields());
                    }
                    default -> throw new IllegalArgumentException();
                };
            }
        }
        throw new IllegalArgumentException("Not found field: " + field + " in input fields: " + inputFields);
    }

    public static List<Schema.Field> setFieldType(
            final List<Schema.Field> inputFields,
            String fieldPath,
            Schema.FieldType fieldType_) {

        final String fieldName;
        if(fieldPath.contains(".")) {
            final String[] fields = fieldPath.split("\\.", 2);
            fieldName = fields[0];
            fieldPath = fields[1];
        } else {
            fieldName = fieldPath;
        }
        final Schema.FieldType fieldType = getInputFieldType(fieldName, inputFields);

        final List<Schema.Field> resultFields = new ArrayList<>();
        for(final Schema.Field inputField : inputFields) {
            if(!inputField.getName().equals(fieldName)) {
                resultFields.add(inputField);
            } else if(fieldName.equals(fieldPath)) {
                resultFields.add(Schema.Field.of(inputField.getName(), fieldType_));
            } else {
                switch (fieldType.getType()) {
                    case element -> {
                        final List<Schema.Field> childFields = setFieldType(
                                fieldType.getElementSchema().getFields(), fieldPath, fieldType_);
                        final Schema.Field childField = Schema.Field.of(fieldName, Schema.FieldType.element(Schema.of(childFields)));
                        resultFields.add(childField);
                    }
                    case array -> {
                        if (!Schema.Type.element.equals(fieldType.getArrayValueType().getType())) {
                            throw new IllegalArgumentException();
                        }
                        final List<Schema.Field> childFields = setFieldType(
                                fieldType.getArrayValueType().getElementSchema().getFields(), fieldPath, fieldType_);
                        final Schema.Field childField = Schema.Field.of(fieldName, Schema.FieldType.element(Schema.of(childFields)));
                        resultFields.add(childField);
                        System.out.println("oijsdlkjflaksjdflajfd");
                    }
                    default -> throw new IllegalArgumentException();
                }
            }
        }
        return resultFields;
    }

    public static Object getValue(Map<?, ?> input, String field) {
        if(input.containsKey(field)) {
            return input.get(field);
        } else if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            final Object value = input.get(fields[0]);
            return switch (value) {
                case Map<?, ?> map -> getValue(map, fields[1]);
                case String str -> {
                    try {
                        final JsonElement jsonElement = new Gson().fromJson(str, JsonElement.class);
                        if (jsonElement.isJsonObject()) {
                            final Map<String, Object> map = JsonToMapConverter.convert(jsonElement);
                            yield getValue(map, fields[1]);
                        } else if (jsonElement.isJsonArray()) {
                            List<Object> list = new ArrayList<>();
                            for (final JsonElement child : jsonElement.getAsJsonArray()) {
                                if (child.isJsonObject()) {
                                    final Map<String, Object> map = JsonToMapConverter.convert(child);
                                    final Object childValue = getValue(map, fields[1]);
                                    list.add(childValue);
                                } else {
                                    list.add(null);
                                }
                            }
                            yield list;
                        } else {
                            yield null;
                        }
                    } catch (Throwable e) {
                        throw new IllegalArgumentException("Failed to get field: " + field + ", value: " + str);
                    }
                }
                case List<?> l -> {
                    final List<Object> list = new ArrayList<>();
                    for (final Object child : l) {
                        final Object childValue = switch (child) {
                            case Map<?, ?> map -> getValue(map, fields[1]);
                            default -> throw new IllegalArgumentException("Illegal nested field: " + field + ", value of array: " + child);
                        };
                        list.add(childValue);
                    }
                    yield list;
                }
                case null -> null;
                default -> throw new IllegalArgumentException("Illegal nested field: " + field + ", value: " + value);
            };
        } else {
            return null;
        }
    }

    public static void setValue(Map<String, Object> input, String field, Object value_) {
        if(input.containsKey(field)) {
            input.put(field, value_);
        } else if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            final Object value = input.get(fields[0]);
            switch (value) {
                case Map map -> {
                    setValue(map, fields[1], value_);
                }
                default -> throw new IllegalArgumentException("Illegal nested field: " + field + ", value: " + value);
            }
        }
    }

    public static Object getAsPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        if(primitiveValue == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool -> switch (primitiveValue) {
                case Boolean b -> b;
                case String s -> Boolean.valueOf(s);
                case Number n -> n.doubleValue() > 0;
                default -> null;
            };
            case string, json -> switch (primitiveValue) {
                case String s -> s;
                case byte[] b -> new String(b, StandardCharsets.UTF_8);
                case ByteBuffer bb -> new String(bb.array(), StandardCharsets.UTF_8);
                case Object o -> o.toString();
            };
            case bytes -> switch (primitiveValue) {
                case ByteBuffer bb -> bb;
                case byte[] b -> ByteBuffer.wrap(b);
                case String s -> ByteBuffer.wrap(Base64.getDecoder().decode(s));
                default -> null;
            };
            case int16 -> switch (primitiveValue) {
                case Short s -> s;
                case Number n -> n.shortValue();
                case String s -> Short.parseShort(s);
                default -> null;
            };
            case int32 -> switch (primitiveValue) {
                case Integer i -> i;
                case Number n -> n.intValue();
                case String s -> Integer.parseInt(s);
                default -> null;
            };
            case int64 -> switch (primitiveValue) {
                case Long l -> l;
                case Number n -> n.longValue();
                case String s -> Long.parseLong(s);
                default -> null;
            };
            case float32 -> switch (primitiveValue) {
                case Float f -> f;
                case Number n -> n.floatValue();
                case String s -> Float.parseFloat(s);
                default -> null;
            };
            case float64 -> switch (primitiveValue) {
                case Double d -> d;
                case Number n -> n.doubleValue();
                case String s -> Double.parseDouble(s);
                default -> null;
            };
            case date -> switch (primitiveValue) {
                case String s -> Long.valueOf(DateTimeUtil.toLocalDate(s).toEpochDay()).intValue();
                case Number n -> n.intValue();
                default -> null;
            };
            case time -> switch (primitiveValue) {
                case String s -> DateTimeUtil.toLocalTime(s).toNanoOfDay() / 1000L;
                case Number n -> n.intValue();
                default -> null;
            };
            case timestamp -> switch (primitiveValue) {
                case String s -> DateTimeUtil.toEpochMicroSecond(s);
                case Number n -> n.longValue();
                default -> null;
            };
            case enumeration -> switch (primitiveValue) {
                case String s -> fieldType.getSymbolIndex(s);
                case Integer i -> i;
                case Number n -> n.intValue();
                default -> null;
            };
            case element, map -> switch (primitiveValue) {
                case Map<?,?> m -> m;
                case String s -> {
                    try{
                        final JsonElement jsonElement = new Gson().fromJson(s, JsonElement.class);
                        if (jsonElement.isJsonObject()) {
                            yield JsonToMapConverter.convert(jsonElement);
                        }
                        yield null;
                    } catch (Throwable e) {
                        yield null;
                    }
                }
                default -> null;
            };
            case array -> switch (primitiveValue) {
                case List<?> list -> list;
                case String s -> {
                    try{
                        final JsonElement jsonElement = new Gson().fromJson(s, JsonElement.class);
                        if (jsonElement.isJsonArray()) {
                            final List<Object> list = new ArrayList<>();
                            for(final JsonElement e : jsonElement.getAsJsonArray()) {
                                final Object o = getAsPrimitive(fieldType.getArrayValueType(), e.toString());
                                list.add(o);
                            }
                            yield list;
                        }
                        yield null;
                    } catch (Throwable e) {
                        yield null;
                    }
                }
                default -> null;
            };
            default -> null;
        };
    }

    public static Map<String, Object> deepCopyMap(final Map<String, Object> original) {
        if (original == null) {
            return null;
        }
        final Map<String, Object> copy = new HashMap<>();
        for (final Map.Entry<String, Object> entry : original.entrySet()) {
            copy.put(entry.getKey(), deepCopyObject(entry.getValue()));
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    private static Object deepCopyObject(Object obj) {
        if (obj == null) {
            return null;
        }

        return switch (obj) {
            case Number n -> n;
            case String s -> s;
            case Boolean b -> b;
            case byte[] b -> Arrays.copyOf(b, b.length);
            case ByteBuffer bytes -> {
                ByteBuffer copyBuffer = bytes.isDirect() ?
                        ByteBuffer.allocateDirect(bytes.capacity()) :
                        ByteBuffer.allocate(bytes.capacity());
                ByteBuffer readOnlyCopy = bytes.duplicate();
                readOnlyCopy.clear();
                copyBuffer.put(readOnlyCopy);
                copyBuffer.position(bytes.position());
                copyBuffer.limit(bytes.limit());
                yield copyBuffer;
            }
            case Map<?, ?> map -> deepCopyMap((Map<String, Object>) map);
            case List<?> list -> {
                List<Object> copyList = new ArrayList<>(list.size());
                for (Object item : list) {
                    copyList.add(deepCopyObject(item));
                }
                yield copyList;
            }
            default -> throw new IllegalArgumentException("Unsupported type encountered during deep copy:" + obj.getClass().getName());
        };
    }

}
