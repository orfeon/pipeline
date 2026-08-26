package com.mercari.solution.util.pipeline.cdc;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Destination DDL generation for schema changes carried by the envelope ({@code SCHEMA} /
 * {@code TRUNCATE} control records), and the reverse mapping of a destination table schema
 * onto {@link ChangeSchema} columns (the schema drift baseline).
 *
 * <p>Only additive changes are generated (one {@code ADD COLUMN IF NOT EXISTS} per new
 * column, always NULLABLE): the statements are idempotent, so the same change reported by
 * several workers, a replay, or a retry never fails. Type changes, drops and key changes are
 * never turned into DDL.</p>
 */
public class ChangeDdl {

    public enum Dialect {
        bigquery
    }

    private ChangeDdl() {
    }

    /** The outcome of comparing a table's previous and current row schema. */
    public record Diff(
            List<ChangeSchema.Column> added,
            List<ChangeSchema.Column> typeChanged,
            List<ChangeSchema.Column> dropped,
            List<ChangeSchema.Column> addedKeys) {

        public boolean isEmpty() {
            return added.isEmpty() && typeChanged.isEmpty() && dropped.isEmpty() && addedKeys.isEmpty();
        }
    }

    /** Compares by column name and type; key flags only matter for added columns. */
    public static Diff diff(final List<ChangeSchema.Column> previous, final List<ChangeSchema.Column> current) {
        final List<ChangeSchema.Column> added = new ArrayList<>();
        final List<ChangeSchema.Column> addedKeys = new ArrayList<>();
        final List<ChangeSchema.Column> typeChanged = new ArrayList<>();
        final List<ChangeSchema.Column> dropped = new ArrayList<>();
        if(current != null) {
            for(final ChangeSchema.Column column : current) {
                final ChangeSchema.Column p = find(previous, column.name());
                if(p == null) {
                    if(column.key()) {
                        addedKeys.add(column);
                    } else {
                        added.add(column);
                    }
                } else if(!p.type().equalsIgnoreCase(column.type())) {
                    typeChanged.add(column);
                }
            }
        }
        if(previous != null) {
            for(final ChangeSchema.Column column : previous) {
                if(find(current, column.name()) == null) {
                    dropped.add(column);
                }
            }
        }
        return new Diff(added, typeChanged, dropped, addedKeys);
    }

    private static ChangeSchema.Column find(final List<ChangeSchema.Column> columns, final String name) {
        if(columns == null) {
            return null;
        }
        for(final ChangeSchema.Column column : columns) {
            if(column.name().equals(name)) {
                return column;
            }
        }
        return null;
    }

    /**
     * {@code ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `c` <type>;} per added column, or null
     * when there is nothing to add.
     */
    public static String addColumns(final Dialect dialect, final String table, final List<ChangeSchema.Column> added) {
        if(added == null || added.isEmpty()) {
            return null;
        }
        final StringBuilder sb = new StringBuilder();
        for(final ChangeSchema.Column column : added) {
            if(!sb.isEmpty()) {
                sb.append('\n');
            }
            switch (dialect) {
                case bigquery -> sb
                        .append("ALTER TABLE ").append(quoteBigQuery(table))
                        .append(" ADD COLUMN IF NOT EXISTS ").append(quoteBigQueryIdentifier(column.name()))
                        .append(' ').append(toBigQueryType(column.type()))
                        .append(';');
            }
        }
        return sb.toString();
    }

    public static String truncate(final Dialect dialect, final String table) {
        return switch (dialect) {
            case bigquery -> "TRUNCATE TABLE " + quoteBigQuery(table) + ";";
        };
    }

    /** Unified envelope type name to a BigQuery GoogleSQL DDL type. */
    public static String toBigQueryType(final String type) {
        if(type == null) {
            return "STRING";
        }
        final String upper = type.trim().toUpperCase(Locale.ROOT);
        if(upper.startsWith("ARRAY<") && upper.endsWith(">")) {
            return "ARRAY<" + toBigQueryType(upper.substring(6, upper.length() - 1)) + ">";
        }
        return switch (upper) {
            case ChangeSchema.TYPE_BOOL -> "BOOL";
            case ChangeSchema.TYPE_INT64 -> "INT64";
            case ChangeSchema.TYPE_FLOAT32, ChangeSchema.TYPE_FLOAT64 -> "FLOAT64";
            case ChangeSchema.TYPE_NUMERIC -> "NUMERIC";
            case ChangeSchema.TYPE_BYTES -> "BYTES";
            case ChangeSchema.TYPE_DATE -> "DATE";
            case ChangeSchema.TYPE_DATETIME -> "DATETIME";
            case ChangeSchema.TYPE_TIMESTAMP -> "TIMESTAMP";
            case ChangeSchema.TYPE_JSON -> "JSON";
            case ChangeSchema.TYPE_STRING, ChangeSchema.TYPE_UUID -> "STRING"; // BigQuery has no UUID type
            default -> upper.startsWith("STRUCT<") ? "JSON" : "STRING"; // unknown / nested: keep the value as text
        };
    }

    /** BigQuery table schema (as fetched from the service) to envelope columns. Key flags are unknown (false). */
    public static List<ChangeSchema.Column> fromBigQuerySchema(final TableSchema schema) {
        if(schema == null || schema.getFields() == null) {
            return null;
        }
        final List<ChangeSchema.Column> columns = new ArrayList<>();
        for(final TableFieldSchema field : schema.getFields()) {
            columns.add(new ChangeSchema.Column(field.getName(), fromBigQueryType(field), false));
        }
        return columns;
    }

    private static String fromBigQueryType(final TableFieldSchema field) {
        final String type = field.getType() == null ? "STRING" : field.getType().toUpperCase(Locale.ROOT);
        final String scalar = switch (type) {
            case "BOOL", "BOOLEAN" -> ChangeSchema.TYPE_BOOL;
            case "INT64", "INTEGER" -> ChangeSchema.TYPE_INT64;
            case "FLOAT64", "FLOAT" -> ChangeSchema.TYPE_FLOAT64;
            case "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL" -> ChangeSchema.TYPE_NUMERIC;
            case "BYTES" -> ChangeSchema.TYPE_BYTES;
            case "DATE" -> ChangeSchema.TYPE_DATE;
            case "DATETIME" -> ChangeSchema.TYPE_DATETIME;
            case "TIMESTAMP" -> ChangeSchema.TYPE_TIMESTAMP;
            case "JSON" -> ChangeSchema.TYPE_JSON;
            case "RECORD", "STRUCT" -> "STRUCT<>";
            default -> ChangeSchema.TYPE_STRING;
        };
        return "REPEATED".equalsIgnoreCase(field.getMode()) ? "ARRAY<" + scalar + ">" : scalar;
    }

    private static String quoteBigQuery(final String table) {
        final String t = table.trim();
        if(t.startsWith("`")) {
            return t;
        }
        return "`" + t.replace(':', '.') + "`";
    }

    private static String quoteBigQueryIdentifier(final String name) {
        return "`" + name.replace("`", "") + "`";
    }

}
