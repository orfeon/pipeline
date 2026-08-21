package com.mercari.solution.util.domain.text.template;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

/** {@code utils.json.*} template functions: serialize template values (maps, lists, scalars) as JSON text. */
public class JsonFunctions {

    /** JSON text of a value (record map, list, scalar). Temporal values become ISO-8601 strings, bytes Base64. */
    public String toJson(final Object value) {
        return toJsonElement(value).toString();
    }

    /** JSON string literal (quoted and escaped) of a text value. */
    public String quote(final Object value) {
        return value == null ? "null" : new JsonPrimitive(value.toString()).toString();
    }

    static JsonElement toJsonElement(final Object value) {
        return switch (value) {
            case null -> JsonNull.INSTANCE;
            case JsonElement e -> e;
            case Boolean b -> new JsonPrimitive(b);
            case Number n -> new JsonPrimitive(n);
            case String s -> new JsonPrimitive(s);
            case Character c -> new JsonPrimitive(c);
            case byte[] b -> new JsonPrimitive(Base64.getEncoder().encodeToString(b));
            case ByteBuffer b -> {
                final byte[] bytes = new byte[b.remaining()];
                b.duplicate().get(bytes);
                yield new JsonPrimitive(Base64.getEncoder().encodeToString(bytes));
            }
            case Map<?, ?> map -> {
                final JsonObject o = new JsonObject();
                for(final Map.Entry<?, ?> entry : map.entrySet()) {
                    o.add(String.valueOf(entry.getKey()), toJsonElement(entry.getValue()));
                }
                yield o;
            }
            case Collection<?> list -> {
                final JsonArray a = new JsonArray();
                for(final Object v : list) {
                    a.add(toJsonElement(v));
                }
                yield a;
            }
            case Object[] array -> {
                final JsonArray a = new JsonArray();
                for(final Object v : array) {
                    a.add(toJsonElement(v));
                }
                yield a;
            }
            default -> new JsonPrimitive(value.toString());
        };
    }
}
