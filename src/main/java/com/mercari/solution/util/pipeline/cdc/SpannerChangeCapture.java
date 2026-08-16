package com.mercari.solution.util.pipeline.cdc;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
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
 * Spanner change stream provider support: the provider-native record schema emitted by the
 * spanner source's {@code changeDataCapture} mode, and its normalization into the unified
 * {@link ChangeRecord} envelope (used by the {@code cdc} transform).
 */
public class SpannerChangeCapture {

    public static final String PROVIDER = "spanner";

    private static final String RESOURCE_AVRO_SCHEMA_PATH = "/schema/avro/spanner_cdc.avsc";
    private static final String RESOURCE_RUNTIME_AVRO_SCHEMA_PATH = "/template/MPipeline/resources/schema/avro/spanner_cdc.avsc";

    public static final String FIELD_PARTITION_TOKEN = "partitionToken";
    public static final String FIELD_COMMIT_TIMESTAMP = "commitTimestamp";
    public static final String FIELD_SERVER_TRANSACTION_ID = "serverTransactionId";
    public static final String FIELD_IS_LAST_RECORD = "isLastRecordInTransactionInPartition";
    public static final String FIELD_RECORD_SEQUENCE = "recordSequence";
    public static final String FIELD_TABLE_NAME = "tableName";
    public static final String FIELD_ROW_TYPE = "rowType";
    public static final String FIELD_MODS = "mods";
    public static final String FIELD_MOD_TYPE = "modType";
    public static final String FIELD_VALUE_CAPTURE_TYPE = "valueCaptureType";
    public static final String FIELD_TRANSACTION_TAG = "transactionTag";
    public static final String FIELD_IS_SYSTEM_TRANSACTION = "isSystemTransaction";
    public static final String FIELD_KEYS_JSON = "keysJson";
    public static final String FIELD_OLD_VALUES_JSON = "oldValuesJson";
    public static final String FIELD_NEW_VALUES_JSON = "newValuesJson";

    private SpannerChangeCapture() {
    }

    /** The provider-native output schema of the spanner source {@code changeDataCapture} mode. */
    public static Schema schema() {
        try(final InputStream is = SpannerChangeCapture.class.getResourceAsStream(RESOURCE_AVRO_SCHEMA_PATH)) {
            if(is == null) {
                try(final InputStream iss = Files.newInputStream(Path.of(RESOURCE_RUNTIME_AVRO_SCHEMA_PATH))) {
                    return Schema.of(AvroSchemaUtil.convertSchema(new String(iss.readAllBytes(), StandardCharsets.UTF_8)));
                } catch (final Throwable e) {
                    throw new IllegalArgumentException("spanner cdc avro schema file is not found", e);
                }
            }
            return Schema.of(AvroSchemaUtil.convertSchema(new String(is.readAllBytes(), StandardCharsets.UTF_8)));
        } catch (final IOException e) {
            throw new IllegalArgumentException("spanner cdc avro schema file could not be loaded", e);
        }
    }

    /** Converts a Beam change stream record into the provider-native schema'd element. */
    public static MElement convert(final DataChangeRecord record, final Instant timestamp) {

        final Map<String, Object> values = new HashMap<>();
        values.put(FIELD_PARTITION_TOKEN, record.getPartitionToken());
        values.put(FIELD_COMMIT_TIMESTAMP, DateTimeUtil.toEpochMicroSecond(record.getCommitTimestamp()));
        values.put(FIELD_SERVER_TRANSACTION_ID, record.getServerTransactionId());
        values.put(FIELD_IS_LAST_RECORD, record.isLastRecordInTransactionInPartition());
        values.put(FIELD_RECORD_SEQUENCE, record.getRecordSequence());
        values.put(FIELD_TABLE_NAME, record.getTableName());

        final List<Map<String, Object>> rowTypes = new ArrayList<>();
        for(final ColumnType columnType : record.getRowType()) {
            final Map<String, Object> rowTypeValues = new HashMap<>();
            rowTypeValues.put("name", columnType.getName());
            rowTypeValues.put("code", extractTypeCode(columnType.getType().getCode()));
            rowTypeValues.put("isPrimaryKey", columnType.isPrimaryKey());
            rowTypeValues.put("ordinalPosition", columnType.getOrdinalPosition());
            rowTypes.add(rowTypeValues);
        }
        values.put(FIELD_ROW_TYPE, rowTypes);

        final List<Map<String, Object>> mods = new ArrayList<>();
        for(final Mod mod : record.getMods()) {
            final Map<String, Object> modValues = new HashMap<>();
            modValues.put(FIELD_KEYS_JSON, mod.getKeysJson());
            modValues.put(FIELD_OLD_VALUES_JSON, mod.getOldValuesJson());
            modValues.put(FIELD_NEW_VALUES_JSON, mod.getNewValuesJson());
            mods.add(modValues);
        }
        values.put(FIELD_MODS, mods);

        values.put(FIELD_MOD_TYPE, record.getModType().name());
        values.put(FIELD_VALUE_CAPTURE_TYPE, record.getValueCaptureType().name());
        values.put(FIELD_TRANSACTION_TAG, record.getTransactionTag());
        values.put(FIELD_IS_SYSTEM_TRANSACTION, record.isSystemTransaction());
        values.put("numberOfRecordsInTransaction", record.getNumberOfRecordsInTransaction());
        values.put("numberOfPartitionsInTransaction", record.getNumberOfPartitionsInTransaction());

        return MElement.of(values, timestamp);
    }

    // TypeCode.getCode() may hold the raw json form {"code":"STRING"}
    private static String extractTypeCode(final String code) {
        if(code == null) {
            return "";
        }
        return code
                .replaceAll("\\{\"code\":", "")
                .replaceAll("}", "")
                .replaceAll("\"", "");
    }

    /**
     * Normalizes one provider-native record (live from the source, or replayed from files)
     * into envelope primitive-value maps — one per entry of {@code mods}.
     */
    public static List<Map<String, Object>> normalize(final MElement element) {

        final Map<String, Object> values = element.asPrimitiveMap();

        final String table = asString(values.get(FIELD_TABLE_NAME));
        if(table == null) {
            throw new IllegalArgumentException("spanner change record misses tableName: " + values);
        }
        final Long commitTimestampMicros = asLong(values.get(FIELD_COMMIT_TIMESTAMP));
        if(commitTimestampMicros == null) {
            throw new IllegalArgumentException("spanner change record misses commitTimestamp: " + values);
        }
        final long recordSequence = parseRecordSequence(asString(values.get(FIELD_RECORD_SEQUENCE)));
        final ChangeRecord.Op op = switch (asString(values.get(FIELD_MOD_TYPE))) {
            case "INSERT" -> ChangeRecord.Op.INSERT;
            case "UPDATE" -> ChangeRecord.Op.UPDATE;
            case "DELETE" -> ChangeRecord.Op.DELETE;
            case null, default -> throw new IllegalArgumentException(
                    "spanner change record has unsupported modType: " + values.get(FIELD_MOD_TYPE));
        };

        final String metadataJson = createSourceMetadataJson(values);

        final List<Map<String, Object>> envelopes = new ArrayList<>();
        final Object modsValue = values.get(FIELD_MODS);
        if(!(modsValue instanceof List<?> mods)) {
            throw new IllegalArgumentException("spanner change record misses mods: " + values);
        }
        int index = 0;
        for(final Object modValue : mods) {
            if(!(modValue instanceof Map<?, ?> mod)) {
                throw new IllegalArgumentException("spanner change record has illegal mod: " + modValue);
            }
            final Map<String, Object> envelope = new HashMap<>();
            envelope.put(ChangeRecord.FIELD_TABLE, table);
            envelope.put(ChangeRecord.FIELD_OP, op.getId());
            envelope.put(ChangeRecord.FIELD_KEYS, emptyToNull(asString(mod.get(FIELD_KEYS_JSON))));
            envelope.put(ChangeRecord.FIELD_BEFORE, emptyToNull(asString(mod.get(FIELD_OLD_VALUES_JSON))));
            envelope.put(ChangeRecord.FIELD_AFTER, emptyToNull(asString(mod.get(FIELD_NEW_VALUES_JSON))));
            envelope.put(ChangeRecord.FIELD_COMMIT_TIMESTAMP, commitTimestampMicros);
            envelope.put(ChangeRecord.FIELD_SEQUENCE, ChangeRecord.sequence(commitTimestampMicros, recordSequence, index));

            final Map<String, Object> source = new HashMap<>();
            source.put(ChangeRecord.FIELD_SOURCE_PROVIDER, PROVIDER);
            source.put(ChangeRecord.FIELD_SOURCE_DATABASE, null);
            source.put(ChangeRecord.FIELD_SOURCE_METADATA, metadataJson);
            envelope.put(ChangeRecord.FIELD_SOURCE, source);

            if(envelope.get(ChangeRecord.FIELD_KEYS) == null) {
                envelope.put(ChangeRecord.FIELD_KEYS, "{}");
            }
            envelopes.add(envelope);
            index++;
        }
        return envelopes;
    }

    private static String createSourceMetadataJson(final Map<String, Object> values) {
        final com.google.gson.JsonObject metadata = new com.google.gson.JsonObject();
        metadata.addProperty(FIELD_SERVER_TRANSACTION_ID, asString(values.get(FIELD_SERVER_TRANSACTION_ID)));
        metadata.addProperty(FIELD_RECORD_SEQUENCE, asString(values.get(FIELD_RECORD_SEQUENCE)));
        metadata.addProperty(FIELD_TRANSACTION_TAG, asString(values.get(FIELD_TRANSACTION_TAG)));
        metadata.addProperty(FIELD_VALUE_CAPTURE_TYPE, asString(values.get(FIELD_VALUE_CAPTURE_TYPE)));
        metadata.addProperty(FIELD_PARTITION_TOKEN, asString(values.get(FIELD_PARTITION_TOKEN)));
        return metadata.toString();
    }

    private static long parseRecordSequence(final String recordSequence) {
        if(recordSequence == null || recordSequence.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(recordSequence.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("spanner change record has non-numeric recordSequence: " + recordSequence, e);
        }
    }

    private static String emptyToNull(final String json) {
        if(json == null) {
            return null;
        }
        final String trimmed = json.trim();
        if(trimmed.isEmpty() || "{}".equals(trimmed)) {
            return null;
        }
        return trimmed;
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
