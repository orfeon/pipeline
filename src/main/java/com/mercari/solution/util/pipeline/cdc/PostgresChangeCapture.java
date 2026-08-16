package com.mercari.solution.util.pipeline.cdc;

import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.db.PgOutput;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL logical replication provider support: the provider-native record schema emitted
 * by the postgres source's {@code changeDataCapture} mode (pgoutput plugin), and its
 * normalization into the unified {@link ChangeRecord} envelope (used by the {@code cdc}
 * transform).
 */
public class PostgresChangeCapture {

    public static final String PROVIDER = "postgres";

    private static final String RESOURCE_AVRO_SCHEMA_PATH = "/schema/avro/postgres_cdc.avsc";
    private static final String RESOURCE_RUNTIME_AVRO_SCHEMA_PATH = "/template/MPipeline/resources/schema/avro/postgres_cdc.avsc";

    public static final String FIELD_LSN = "lsn";
    public static final String FIELD_COMMIT_LSN = "commitLsn";
    public static final String FIELD_COMMIT_TIMESTAMP = "commitTimestamp";
    public static final String FIELD_TRANSACTION_ID = "transactionId";
    public static final String FIELD_SEQUENCE = "sequence";
    public static final String FIELD_DATABASE = "database";
    public static final String FIELD_SCHEMA = "schema";
    public static final String FIELD_TABLE = "table";
    public static final String FIELD_OP = "op";
    public static final String FIELD_KEYS_JSON = "keysJson";
    public static final String FIELD_OLD_VALUES_JSON = "oldValuesJson";
    public static final String FIELD_NEW_VALUES_JSON = "newValuesJson";

    private PostgresChangeCapture() {
    }

    /** The provider-native output schema of the postgres source {@code changeDataCapture} mode. */
    public static Schema schema() {
        try(final InputStream is = PostgresChangeCapture.class.getResourceAsStream(RESOURCE_AVRO_SCHEMA_PATH)) {
            if(is == null) {
                try(final InputStream iss = Files.newInputStream(Path.of(RESOURCE_RUNTIME_AVRO_SCHEMA_PATH))) {
                    return Schema.of(AvroSchemaUtil.convertSchema(new String(iss.readAllBytes(), StandardCharsets.UTF_8)));
                } catch (final Throwable e) {
                    throw new IllegalArgumentException("postgres cdc avro schema file is not found", e);
                }
            }
            return Schema.of(AvroSchemaUtil.convertSchema(new String(is.readAllBytes(), StandardCharsets.UTF_8)));
        } catch (final IOException e) {
            throw new IllegalArgumentException("postgres cdc avro schema file could not be loaded", e);
        }
    }

    /** Converts a decoded pgoutput event into the provider-native schema'd element. */
    public static MElement convert(final PgOutput.ChangeEvent event, final String database, final Instant timestamp) {
        final Map<String, Object> values = new HashMap<>();
        values.put(FIELD_LSN, event.lsn);
        values.put(FIELD_COMMIT_LSN, event.commitLsn);
        values.put(FIELD_COMMIT_TIMESTAMP, event.commitTimestampMicros);
        values.put(FIELD_TRANSACTION_ID, event.transactionId);
        values.put(FIELD_SEQUENCE, event.sequence);
        values.put(FIELD_DATABASE, database);
        values.put(FIELD_SCHEMA, event.schema);
        values.put(FIELD_TABLE, event.table);
        values.put(FIELD_OP, event.op);
        values.put(FIELD_KEYS_JSON, event.keysJson);
        values.put(FIELD_OLD_VALUES_JSON, event.oldValuesJson);
        values.put(FIELD_NEW_VALUES_JSON, event.newValuesJson);
        return MElement.of(values, timestamp);
    }

    /**
     * Normalizes one provider-native record (live from the source, or replayed from files)
     * into envelope primitive-value maps. TRUNCATE events have no per-row representation in
     * the envelope and are skipped.
     */
    public static List<Map<String, Object>> normalize(final MElement element) {

        final Map<String, Object> values = element.asPrimitiveMap();

        final String opValue = asString(values.get(FIELD_OP));
        final ChangeRecord.Op op = switch (opValue) {
            case "INSERT" -> ChangeRecord.Op.INSERT;
            case "UPDATE" -> ChangeRecord.Op.UPDATE;
            case "DELETE" -> ChangeRecord.Op.DELETE;
            case "TRUNCATE" -> null;
            case null, default -> throw new IllegalArgumentException(
                    "postgres change record has unsupported op: " + opValue);
        };
        if(op == null) {
            return new ArrayList<>();
        }

        final String table = asString(values.get(FIELD_TABLE));
        if(table == null) {
            throw new IllegalArgumentException("postgres change record misses table: " + values);
        }
        final String schema = asString(values.get(FIELD_SCHEMA));
        final Long commitTimestampMicros = asLong(values.get(FIELD_COMMIT_TIMESTAMP));
        if(commitTimestampMicros == null) {
            throw new IllegalArgumentException("postgres change record misses commitTimestamp: " + values);
        }
        final Long commitLsn = asLong(values.get(FIELD_COMMIT_LSN));
        if(commitLsn == null) {
            throw new IllegalArgumentException("postgres change record misses commitLsn: " + values);
        }
        final long sequence = asLong(values.get(FIELD_SEQUENCE)) == null ? 0L : asLong(values.get(FIELD_SEQUENCE));

        final Map<String, Object> envelope = new HashMap<>();
        // public-schema tables keep their bare name, matching the batch tables mode tag
        envelope.put(ChangeRecord.FIELD_TABLE, schema == null || "public".equals(schema) ? table : schema + "." + table);
        envelope.put(ChangeRecord.FIELD_OP, op.getId());
        envelope.put(ChangeRecord.FIELD_KEYS, asString(values.get(FIELD_KEYS_JSON)) == null ? "{}" : asString(values.get(FIELD_KEYS_JSON)));
        envelope.put(ChangeRecord.FIELD_BEFORE, asString(values.get(FIELD_OLD_VALUES_JSON)));
        envelope.put(ChangeRecord.FIELD_AFTER, asString(values.get(FIELD_NEW_VALUES_JSON)));
        envelope.put(ChangeRecord.FIELD_COMMIT_TIMESTAMP, commitTimestampMicros);
        // LSNs are 64-bit and monotonic: (commit LSN, change index in tx) yields a total order
        envelope.put(ChangeRecord.FIELD_SEQUENCE, ChangeRecord.sequence(commitLsn, sequence));

        final Map<String, Object> source = new HashMap<>();
        source.put(ChangeRecord.FIELD_SOURCE_PROVIDER, PROVIDER);
        source.put(ChangeRecord.FIELD_SOURCE_DATABASE, asString(values.get(FIELD_DATABASE)));
        source.put(ChangeRecord.FIELD_SOURCE_METADATA, createSourceMetadataJson(values));
        envelope.put(ChangeRecord.FIELD_SOURCE, source);

        final List<Map<String, Object>> envelopes = new ArrayList<>();
        envelopes.add(envelope);
        return envelopes;
    }

    private static String createSourceMetadataJson(final Map<String, Object> values) {
        final JsonObject metadata = new JsonObject();
        metadata.addProperty(FIELD_LSN, asLong(values.get(FIELD_LSN)));
        metadata.addProperty(FIELD_TRANSACTION_ID, asLong(values.get(FIELD_TRANSACTION_ID)));
        metadata.addProperty(FIELD_SCHEMA, asString(values.get(FIELD_SCHEMA)));
        return metadata.toString();
    }

    private static String asString(final Object value) {
        return value == null ? null : value.toString();
    }

    private static Long asLong(final Object value) {
        return switch (value) {
            case Number number -> number.longValue();
            case String string -> Long.parseLong(string);
            case null, default -> null;
        };
    }

}
