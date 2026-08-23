package com.mercari.solution.util.pipeline.cdc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.domain.db.PostgresUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The provider-independent row schema carried by the change record envelope
 * ({@code schema} field, and the {@code SCHEMA} control record).
 *
 * <p>Column types use the envelope's own type names (Spanner GoogleSQL names plus
 * {@code DATETIME}): {@code BOOL INT64 FLOAT32 FLOAT64 NUMERIC STRING BYTES DATE DATETIME
 * TIMESTAMP JSON ARRAY<T> STRUCT<...>}. Each provider maps its native column metadata
 * (Spanner change stream {@code rowType}, pgoutput {@code Relation} columns, canal-json
 * {@code mysqlType}) onto these names so that consumers (destination DDL generation, schema
 * drift detection) never see provider types.</p>
 */
public class ChangeSchema {

    public static final String TYPE_BOOL = "BOOL";
    public static final String TYPE_INT64 = "INT64";
    public static final String TYPE_FLOAT32 = "FLOAT32";
    public static final String TYPE_FLOAT64 = "FLOAT64";
    public static final String TYPE_NUMERIC = "NUMERIC";
    public static final String TYPE_STRING = "STRING";
    public static final String TYPE_BYTES = "BYTES";
    public static final String TYPE_DATE = "DATE";
    public static final String TYPE_DATETIME = "DATETIME";
    public static final String TYPE_TIMESTAMP = "TIMESTAMP";
    public static final String TYPE_JSON = "JSON";

    /** One column of the row schema. */
    public record Column(String name, String type, boolean key) implements Serializable {

        public JsonObject toJson() {
            final JsonObject json = new JsonObject();
            json.addProperty("name", name);
            json.addProperty("type", type);
            json.addProperty("key", key);
            return json;
        }

        public static Column fromJson(final JsonObject json) {
            final String name = json.has("name") && !json.get("name").isJsonNull() ? json.get("name").getAsString() : null;
            final String type = json.has("type") && !json.get("type").isJsonNull() ? json.get("type").getAsString() : TYPE_STRING;
            final boolean key = json.has("key") && !json.get("key").isJsonNull() && json.get("key").getAsBoolean();
            if(name == null) {
                throw new IllegalArgumentException("change schema column requires name: " + json);
            }
            return new Column(name, type, key);
        }
    }

    private ChangeSchema() {
    }

    public static String toJson(final List<Column> columns) {
        if(columns == null) {
            return null;
        }
        final JsonArray array = new JsonArray();
        for(final Column column : columns) {
            array.add(column.toJson());
        }
        return array.toString();
    }

    public static List<Column> fromJson(final String json) {
        if(json == null || json.isBlank()) {
            return null;
        }
        final JsonElement parsed = JsonParser.parseString(json);
        if(!parsed.isJsonArray()) {
            throw new IllegalArgumentException("change schema must be a JSON array: " + json);
        }
        final List<Column> columns = new ArrayList<>();
        for(final JsonElement element : parsed.getAsJsonArray()) {
            if(!element.isJsonObject()) {
                throw new IllegalArgumentException("change schema column must be a JSON object: " + element);
            }
            columns.add(Column.fromJson(element.getAsJsonObject()));
        }
        return columns;
    }

    /**
     * A column-order independent identity of the schema (name, type and key flag of every
     * column). Used to detect schema drift between consecutive change records of a table.
     */
    public static String fingerprint(final List<Column> columns) {
        if(columns == null) {
            return null;
        }
        final List<Column> sorted = new ArrayList<>(columns);
        sorted.sort(Comparator.comparing(Column::name));
        final StringBuilder sb = new StringBuilder();
        for(final Column column : sorted) {
            sb.append(column.name()).append(':').append(column.type()).append(column.key() ? "!" : "").append(',');
        }
        return sb.toString();
    }

    /** Columns present in {@code current} but not in {@code previous} (by name). */
    public static List<Column> addedColumns(final List<Column> previous, final List<Column> current) {
        final List<Column> added = new ArrayList<>();
        if(current == null) {
            return added;
        }
        for(final Column column : current) {
            boolean found = false;
            if(previous != null) {
                for(final Column p : previous) {
                    if(p.name().equals(column.name())) {
                        found = true;
                        break;
                    }
                }
            }
            if(!found) {
                added.add(column);
            }
        }
        return added;
    }

    // ---- provider mappings ----

    /**
     * Spanner change stream {@code rowType} entries (as stored on the provider-native record:
     * {@code name}, {@code code}, {@code isPrimaryKey}). {@code code} is either a bare type code
     * ({@code STRING}), an already normalized {@code ARRAY<STRING>} or the raw TypeCode JSON
     * ({@code {"code":"ARRAY","array_element_type":{"code":"STRING"}}}).
     */
    public static List<Column> fromSpannerRowType(final List<?> rowType) {
        if(rowType == null) {
            return null;
        }
        final List<Column> columns = new ArrayList<>();
        for(final Object value : rowType) {
            if(!(value instanceof Map<?, ?> map)) {
                continue;
            }
            final Object name = map.get("name");
            if(name == null) {
                continue;
            }
            final Object code = map.get("code");
            final Object isPrimaryKey = map.get("isPrimaryKey");
            columns.add(new Column(
                    name.toString(),
                    fromSpannerTypeCode(code == null ? null : code.toString()),
                    Boolean.TRUE.equals(isPrimaryKey) || "true".equals(String.valueOf(isPrimaryKey))));
        }
        return columns;
    }

    public static String fromSpannerTypeCode(final String code) {
        if(code == null || code.isBlank()) {
            return TYPE_STRING;
        }
        final String trimmed = code.trim();
        if(trimmed.startsWith("{")) {
            try {
                final JsonElement parsed = JsonParser.parseString(trimmed);
                if(parsed.isJsonObject()) {
                    return fromSpannerTypeCodeJson(parsed.getAsJsonObject());
                }
            } catch (final RuntimeException e) {
                // fall through: treat as a bare code
            }
        }
        return switch (trimmed.toUpperCase(Locale.ROOT)) {
            case "BOOL" -> TYPE_BOOL;
            case "INT64" -> TYPE_INT64;
            case "FLOAT32" -> TYPE_FLOAT32;
            case "FLOAT64" -> TYPE_FLOAT64;
            case "NUMERIC", "PG_NUMERIC" -> TYPE_NUMERIC;
            case "BYTES" -> TYPE_BYTES;
            case "DATE" -> TYPE_DATE;
            case "TIMESTAMP" -> TYPE_TIMESTAMP;
            case "JSON", "PG_JSONB" -> TYPE_JSON;
            case "STRING" -> TYPE_STRING;
            default -> trimmed.toUpperCase(Locale.ROOT); // ARRAY<...> / STRUCT<...> already normalized, or unknown
        };
    }

    private static String fromSpannerTypeCodeJson(final JsonObject json) {
        final String code = json.has("code") && !json.get("code").isJsonNull() ? json.get("code").getAsString() : null;
        if("ARRAY".equalsIgnoreCase(code)) {
            final JsonElement elementType = json.get("array_element_type");
            final String element = elementType != null && elementType.isJsonObject()
                    ? fromSpannerTypeCodeJson(elementType.getAsJsonObject())
                    : TYPE_STRING;
            return "ARRAY<" + element + ">";
        }
        return fromSpannerTypeCode(code);
    }

    /** pgoutput relation column types (see {@link PostgresUtil.ColumnType}). */
    public static String fromPostgresType(final PostgresUtil.ColumnType type, final PostgresUtil.ColumnType elementType) {
        if(type == null) {
            return TYPE_STRING;
        }
        return switch (type) {
            case BOOL -> TYPE_BOOL;
            case INT2, INT4, INT8 -> TYPE_INT64;
            case FLOAT4 -> TYPE_FLOAT32;
            case FLOAT8 -> TYPE_FLOAT64;
            case NUMERIC -> TYPE_NUMERIC;
            case BYTEA -> TYPE_BYTES;
            case DATE -> TYPE_DATE;
            case TIMESTAMP -> TYPE_DATETIME;
            case TIMESTAMPTZ -> TYPE_TIMESTAMP;
            case JSON, JSONB -> TYPE_JSON;
            case ARRAY -> "ARRAY<" + fromPostgresType(elementType, null) + ">";
            default -> TYPE_STRING; // TEXT VARCHAR BPCHAR TIME TIMETZ UUID XML INET CIDR MACADDR* ENUM
        };
    }

    /**
     * canal-json {@code mysqlType} values ({@code int(11)}, {@code varchar(255)},
     * {@code decimal(10,2) unsigned}, ...).
     */
    public static String fromMysqlType(final String mysqlType) {
        if(mysqlType == null || mysqlType.isBlank()) {
            return TYPE_STRING;
        }
        String base = mysqlType.trim().toLowerCase(Locale.ROOT);
        final int paren = base.indexOf('(');
        if(paren >= 0) {
            base = base.substring(0, paren);
        }
        final int space = base.indexOf(' ');
        if(space >= 0) {
            base = base.substring(0, space);
        }
        return switch (base) {
            case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year", "bit" -> TYPE_INT64;
            case "boolean", "bool" -> TYPE_BOOL;
            case "float" -> TYPE_FLOAT32;
            case "double", "real" -> TYPE_FLOAT64;
            case "decimal", "numeric" -> TYPE_NUMERIC;
            case "date" -> TYPE_DATE;
            case "datetime" -> TYPE_DATETIME;
            case "timestamp" -> TYPE_TIMESTAMP;
            case "json" -> TYPE_JSON;
            case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob" -> TYPE_BYTES;
            default -> TYPE_STRING; // char varchar text enum set time ...
        };
    }

    /**
     * canal-json event column metadata: {@code mysqlType} object ({@code column -> type}) and
     * {@code pkNames}. Column order follows the {@code mysqlType} object.
     */
    public static List<Column> fromTiCdcEvent(final JsonObject mysqlType, final List<String> pkNames) {
        if(mysqlType == null) {
            return null;
        }
        final List<Column> columns = new ArrayList<>();
        for(final Map.Entry<String, JsonElement> entry : mysqlType.entrySet()) {
            final String type = entry.getValue() != null && entry.getValue().isJsonPrimitive()
                    ? entry.getValue().getAsString()
                    : null;
            columns.add(new Column(entry.getKey(), fromMysqlType(type), pkNames != null && pkNames.contains(entry.getKey())));
        }
        return columns;
    }

}
