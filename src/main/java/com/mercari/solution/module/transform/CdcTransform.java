package com.mercari.solution.module.transform;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.BigQueryUtil;
import com.mercari.solution.util.pipeline.cdc.ChangeDdl;
import freemarker.template.Template;
import com.mercari.solution.module.*;
import com.mercari.solution.module.Transform.Module;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.cdc.ChangeRecord;
import com.mercari.solution.util.pipeline.cdc.ChangeSchema;
import com.mercari.solution.util.pipeline.cdc.PostgresChangeCapture;
import com.mercari.solution.util.pipeline.cdc.SpannerChangeCapture;
import com.mercari.solution.util.pipeline.cdc.TiCdcChangeCapture;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Normalizes provider-specific change data capture records into the unified
 * {@link ChangeRecord} envelope. The same normalization applies to live streams
 * (spanner source changeDataCapture mode, kafka) and to records replayed from
 * files archived on GCS/S3 (storage/files source), so archive-then-batch-apply
 * pipelines reuse the exact conversion of the streaming path.
 *
 * <p>Besides the row changes the transform emits table-level control records:
 * {@code TRUNCATE} (postgres / ticdc) and {@code SCHEMA}. A {@code SCHEMA} record is
 * synthesized whenever the row schema carried by a provider record (Spanner
 * {@code rowType}, pgoutput relation columns, canal-json {@code mysqlType}) differs from the
 * one last seen for the table on this worker, so schema drift is visible to consumers even
 * for providers that never emit DDL. Primary key changes are split into DELETE + INSERT.
 * With {@code schemaChanges} configured, SCHEMA / TRUNCATE records additionally carry the
 * destination DDL ({@code statement}) so that an action can apply them.</p>
 *
 * <p>{@code format: envelope} accepts envelope records themselves — archived envelope files,
 * or envelope JSON text in a field (e.g. the {@code record.json} of bigquery sink cdc failure
 * records) — for replay.</p>
 */
@Module(name="cdc")
public class CdcTransform extends Transform {

    private static class Parameters implements Serializable {

        private Format format;
        private String field;
        private Boolean accumulate;
        private Boolean emitSchemaChanges;
        private SchemaChanges schemaChanges;

        private void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(format == null) {
                errorMessages.add("cdc transform module[" + name + "] requires 'format' parameter. one of: " + java.util.Arrays.toString(Format.values()));
            }
            if(schemaChanges != null) {
                errorMessages.addAll(schemaChanges.validate(name));
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(accumulate == null) {
                accumulate = false;
            }
            if(emitSchemaChanges == null) {
                emitSchemaChanges = true;
            }
            if(schemaChanges != null) {
                schemaChanges.setDefaults();
            }
        }
    }

    /** Destination DDL generation for SCHEMA / TRUNCATE control records. */
    private static class SchemaChanges implements Serializable {

        private ChangeDdl.Dialect dialect;
        private String table;
        private OnTypeChange onTypeChange;
        private OnDropColumn onDropColumn;
        private Baseline baseline;

        private List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(dialect == null) {
                errorMessages.add("cdc transform module[" + name + "].schemaChanges requires 'dialect' parameter. one of: " + java.util.Arrays.toString(ChangeDdl.Dialect.values()));
            }
            if(table == null || table.isBlank()) {
                errorMessages.add("cdc transform module[" + name + "].schemaChanges requires 'table' parameter (the destination table name, may be a template on ${table})");
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(onTypeChange == null) {
                onTypeChange = OnTypeChange.skip;
            }
            if(onDropColumn == null) {
                onDropColumn = OnDropColumn.skip;
            }
            if(baseline == null) {
                baseline = Baseline.destination;
            }
        }
    }

    private enum OnTypeChange { skip, fail }
    private enum OnDropColumn { skip }
    private enum Baseline { destination, none }

    private enum Format {
        spanner,
        postgres,
        ticdc,
        envelope
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema outputSchema = ChangeRecord.schema();

        final String jsonField;
        final boolean jsonFieldIsBytes;
        if(Format.ticdc.equals(parameters.format)) {
            jsonField = resolveJsonField(parameters, inputSchema);
            jsonFieldIsBytes = inputSchema.hasField(jsonField)
                    && Schema.Type.bytes.equals(inputSchema.getField(jsonField).getFieldType().getType());
        } else if(Format.envelope.equals(parameters.format)) {
            // envelope records themselves (no field), or envelope JSON text in a (possibly nested) field
            jsonField = parameters.field;
            jsonFieldIsBytes = jsonField != null && inputSchema.hasField(jsonField)
                    && Schema.Type.bytes.equals(inputSchema.getField(jsonField).getFieldType().getType());
            if(jsonField == null) {
                for(final String field : List.of(ChangeRecord.FIELD_TABLE, ChangeRecord.FIELD_OP, ChangeRecord.FIELD_SEQUENCE)) {
                    if(!inputSchema.hasField(field)) {
                        throw new IllegalModuleException(
                                "cdc transform module[" + getName() + "] with format 'envelope' requires envelope records as input (missing field: " + field + "), or 'field' pointing at the envelope JSON text");
                    }
                }
            }
        } else {
            jsonField = null;
            jsonFieldIsBytes = false;
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Normalize", ParDo
                        .of(new NormalizeDoFn(parameters.format, jsonField, jsonFieldIsBytes, parameters.emitSchemaChanges, parameters.schemaChanges, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        PCollection<MElement> output = outputs.get(outputTag)
                .setCoder(ElementCoder.of(outputSchema));

        if(parameters.accumulate) {
            output = accumulate(output, outputSchema);
        }

        return MCollectionTuple
                .of(output, outputSchema);
    }

    /**
     * Collapses the row changes of each (table, keys) to the latest by sequence. TRUNCATE
     * control records act as a barrier: row changes of the table sequenced before the latest
     * TRUNCATE are dropped. Control records themselves pass through unchanged.
     */
    private static PCollection<MElement> accumulate(final PCollection<MElement> envelopes, final Schema outputSchema) {

        final TupleTag<MElement> rowTag = new TupleTag<>() {};
        final TupleTag<MElement> controlTag = new TupleTag<>() {};
        final PCollectionTuple split = envelopes
                .apply("SplitControl", ParDo
                        .of(new SplitControlDoFn(rowTag, controlTag))
                        .withOutputTags(rowTag, TupleTagList.of(controlTag)));
        final PCollection<MElement> rows = split.get(rowTag).setCoder(ElementCoder.of(outputSchema));
        final PCollection<MElement> controls = split.get(controlTag).setCoder(ElementCoder.of(outputSchema));

        final PCollectionView<Map<String, String>> truncates = controls
                .apply("TruncateSequences", ParDo.of(new TruncateSequenceDoFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .apply("LatestTruncatePerTable", Combine.perKey(new MaxSequenceFn()))
                .apply("AsTruncateMap", View.asMap());

        final PCollection<MElement> latest = rows
                .apply("WithTableKeys", ParDo.of(new WithTableKeysDoFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(outputSchema)))
                .apply("GroupByTableKeys", GroupByKey.create())
                .apply("SelectLatest", ParDo
                        .of(new SelectLatestDoFn(truncates))
                        .withSideInputs(truncates))
                .setCoder(ElementCoder.of(outputSchema));

        return PCollectionList.of(latest).and(controls)
                .apply("FlattenAccumulated", Flatten.pCollections())
                .setCoder(ElementCoder.of(outputSchema));
    }

    private String resolveJsonField(final Parameters parameters, final Schema inputSchema) {
        if(parameters.field != null) {
            if(!inputSchema.hasField(parameters.field)) {
                throw new IllegalModuleException(
                        "cdc transform module[" + getName() + "].field: " + parameters.field + " does not exist in input schema");
            }
            return parameters.field;
        }
        // kafka/pubsub message payload, files source content
        for(final String candidate : List.of("payload", "content")) {
            if(inputSchema.hasField(candidate)) {
                return candidate;
            }
        }
        throw new IllegalModuleException(
                "cdc transform module[" + getName() + "] with format 'ticdc' requires 'field' parameter (the input field carrying canal-json event text)");
    }

    private static class NormalizeDoFn extends DoFn<MElement, MElement> {

        private final Format format;
        private final String jsonField;
        private final boolean jsonFieldIsBytes;
        private final boolean emitSchemaChanges;
        private final SchemaChanges schemaChanges;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        // worker-local: the last row schema seen per table. A schema change is therefore
        // reported once per worker (consumers must be idempotent) and — without a destination
        // baseline — the first observation after a (re)start never reports.
        private transient Map<String, List<ChangeSchema.Column>> schemas;
        private transient Map<String, List<ChangeSchema.Column>> previousSchemas;
        private transient Map<String, String> schemaFingerprints;
        private transient Template destinationTemplate;

        NormalizeDoFn(
                final Format format,
                final String jsonField,
                final boolean jsonFieldIsBytes,
                final boolean emitSchemaChanges,
                final SchemaChanges schemaChanges,
                final List<Logging> loggings,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.format = format;
            this.jsonField = jsonField;
            this.jsonFieldIsBytes = jsonFieldIsBytes;
            this.emitSchemaChanges = emitSchemaChanges;
            this.schemaChanges = schemaChanges;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.schemas = new HashMap<>();
            this.previousSchemas = new HashMap<>();
            this.schemaFingerprints = new HashMap<>();
            if(schemaChanges != null && TemplateUtil.isTemplateText(schemaChanges.table)) {
                this.destinationTemplate = TemplateUtil.createStrictTemplate("cdcDestinationTable", schemaChanges.table);
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                Logging.log(LOG, logs, "input", input);
                final List<Map<String, Object>> envelopes = switch (format) {
                    case spanner -> normalizeSpanner(input);
                    case postgres -> normalizePostgres(input);
                    case ticdc -> normalizeTiCdc(input);
                    case envelope -> normalizeEnvelope(input);
                };
                for(final Map<String, Object> envelope : envelopes) {
                    withDestinationStatement(envelope);
                    for(final Map<String, Object> e : ChangeRecord.splitKeyChange(envelope)) {
                        final MElement output = MElement.of(e, c.timestamp());
                        c.output(output);
                        Logging.log(LOG, logs, "output", output);
                    }
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to normalize change record", input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        private List<Map<String, Object>> normalizeSpanner(final MElement input) {
            final List<Map<String, Object>> envelopes = new ArrayList<>();
            final String schemaJson = detectSchemaChange(
                    input.getAsString(SpannerChangeCapture.FIELD_TABLE_NAME), SpannerChangeCapture.columns(input));
            if(schemaJson != null) {
                envelopes.add(SpannerChangeCapture.schemaChange(input, schemaJson));
            }
            envelopes.addAll(SpannerChangeCapture.normalize(input));
            return envelopes;
        }

        private List<Map<String, Object>> normalizePostgres(final MElement input) {
            final List<Map<String, Object>> envelopes = new ArrayList<>();
            final String schemaJson = detectSchemaChange(
                    input.getAsString(PostgresChangeCapture.FIELD_SCHEMA) + "." + input.getAsString(PostgresChangeCapture.FIELD_TABLE),
                    PostgresChangeCapture.columns(input));
            if(schemaJson != null) {
                envelopes.add(PostgresChangeCapture.schemaChange(input, schemaJson));
            }
            envelopes.addAll(PostgresChangeCapture.normalize(input));
            return envelopes;
        }

        private List<Map<String, Object>> normalizeTiCdc(final MElement input) {
            final String content = extractText(input);
            final List<Map<String, Object>> envelopes = new ArrayList<>();
            if(content == null) {
                return envelopes;
            }
            // TiCDC storage sink files are newline-delimited events; a Kafka payload is a single line
            for(final String line : content.split("\n")) {
                if(line.isBlank()) {
                    continue;
                }
                final JsonObject event = TiCdcChangeCapture.parse(line.trim());
                final String schemaJson = detectSchemaChange(TiCdcChangeCapture.table(event), TiCdcChangeCapture.columns(event));
                if(schemaJson != null) {
                    envelopes.add(TiCdcChangeCapture.schemaChange(event, schemaJson));
                }
                envelopes.addAll(TiCdcChangeCapture.normalize(event));
            }
            return envelopes;
        }

        private List<Map<String, Object>> normalizeEnvelope(final MElement input) {
            final List<Map<String, Object>> envelopes = new ArrayList<>();
            if(jsonField == null) {
                envelopes.add(ChangeRecord.fromElement(input));
                return envelopes;
            }
            final String content = extractText(input);
            if(content == null || content.isBlank()) {
                return envelopes;
            }
            envelopes.add(ChangeRecord.fromJson(content));
            return envelopes;
        }

        // Returns the new schema JSON when the table's row schema differs from the last one
        // seen on this worker (null when unchanged, or on the first observation without a
        // destination baseline).
        private String detectSchemaChange(final String table, final List<ChangeSchema.Column> columns) {
            if(!emitSchemaChanges || table == null || columns == null) {
                return null;
            }
            final String fingerprint = ChangeSchema.fingerprint(columns);
            String previousFingerprint = schemaFingerprints.get(table);
            if(previousFingerprint == null) {
                // first observation on this worker: seed from the destination when configured
                final List<ChangeSchema.Column> baseline = loadBaseline(table, columns);
                if(baseline != null) {
                    previousFingerprint = ChangeSchema.fingerprint(baseline);
                    schemas.put(table, baseline);
                }
            }
            final List<ChangeSchema.Column> previous = schemas.put(table, columns);
            previousSchemas.put(table, previous);
            schemaFingerprints.put(table, fingerprint);
            if(previousFingerprint == null || previousFingerprint.equals(fingerprint)) {
                return null;
            }
            final ChangeDdl.Diff diff = ChangeDdl.diff(previous, columns);
            LOG.info("cdc transform detected schema change of table: {} (added: {}, typeChanged: {}, dropped: {}, addedKeys: {})",
                    table, diff.added(), diff.typeChanged(), diff.dropped(), diff.addedKeys());
            if(schemaChanges != null) {
                if(!diff.typeChanged().isEmpty() && OnTypeChange.fail.equals(schemaChanges.onTypeChange)) {
                    throw new IllegalStateException("cdc transform detected column type changes of table: " + table + ": " + diff.typeChanged() + " (schemaChanges.onTypeChange: fail)");
                }
                if(!diff.addedKeys().isEmpty()) {
                    LOG.warn("cdc transform detected new key columns of table: {}: {} — no DDL is generated for key columns", table, diff.addedKeys());
                }
            }
            return ChangeSchema.toJson(columns);
        }

        // The destination table's current schema as the comparison baseline of a table first
        // seen on this worker, with key flags taken from the source columns (the destination
        // schema does not expose them). Null when not configured, or the table does not exist.
        private List<ChangeSchema.Column> loadBaseline(final String table, final List<ChangeSchema.Column> sourceColumns) {
            if(schemaChanges == null || !Baseline.destination.equals(schemaChanges.baseline)) {
                return null;
            }
            final String destination = destinationTable(table);
            try {
                final List<ChangeSchema.Column> columns = switch (schemaChanges.dialect) {
                    case bigquery -> ChangeDdl.fromBigQuerySchema(BigQueryUtil.getTableSchemaFromTable(destination, null));
                };
                if(columns == null) {
                    return null;
                }
                final List<ChangeSchema.Column> withKeys = new ArrayList<>();
                for(final ChangeSchema.Column column : columns) {
                    boolean key = false;
                    for(final ChangeSchema.Column source : sourceColumns) {
                        if(source.name().equals(column.name())) {
                            key = source.key();
                            break;
                        }
                    }
                    withKeys.add(new ChangeSchema.Column(column.name(), column.type(), key));
                }
                LOG.info("cdc transform loaded the destination schema of table: {} as the schema baseline of: {}", destination, table);
                return withKeys;
            } catch (final RuntimeException e) {
                LOG.warn("cdc transform could not load the destination schema of table: {} (baseline for: {}): {}", destination, table, e.getMessage());
                return null;
            }
        }

        // SCHEMA / TRUNCATE records: generate the destination DDL into `statement`. A provider
        // DDL text (ticdc) moves to source.metadata.ddl so that `statement` only ever carries
        // statements of the configured dialect.
        private void withDestinationStatement(final Map<String, Object> envelope) {
            if(schemaChanges == null) {
                return;
            }
            final ChangeRecord.Op op = ChangeRecord.getOp(envelope.get(ChangeRecord.FIELD_OP));
            if(!ChangeRecord.Op.SCHEMA.equals(op) && !ChangeRecord.Op.TRUNCATE.equals(op)) {
                return;
            }
            final Object providerStatement = envelope.get(ChangeRecord.FIELD_STATEMENT);
            if(providerStatement != null && envelope.get(ChangeRecord.FIELD_SOURCE) instanceof Map<?, ?> source) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> s = (Map<String, Object>) source;
                final Object metadata = s.get(ChangeRecord.FIELD_SOURCE_METADATA);
                final JsonObject metadataJson = metadata == null ? new JsonObject() : JsonParser.parseString(metadata.toString()).getAsJsonObject();
                metadataJson.addProperty("ddl", providerStatement.toString());
                s.put(ChangeRecord.FIELD_SOURCE_METADATA, metadataJson.toString());
            }
            final String table = envelope.get(ChangeRecord.FIELD_TABLE).toString();
            final String destination = destinationTable(table);
            final String statement = switch (op) {
                case TRUNCATE -> ChangeDdl.truncate(schemaChanges.dialect, destination);
                case SCHEMA -> {
                    final List<ChangeSchema.Column> columns = ChangeSchema.fromJson((String) envelope.get(ChangeRecord.FIELD_SCHEMA));
                    if(columns == null) {
                        yield null; // provider DDL event without column types
                    }
                    // the previous schema of the table is the one replaced by detectSchemaChange
                    // just before; recompute the additive diff against the destination-side view
                    final List<ChangeSchema.Column> previous = previousSchemas.get(table);
                    yield ChangeDdl.addColumns(schemaChanges.dialect, destination, ChangeDdl.diff(previous, columns).added());
                }
                default -> null;
            };
            envelope.put(ChangeRecord.FIELD_STATEMENT, statement);
        }

        private String destinationTable(final String table) {
            if(destinationTemplate == null) {
                return schemaChanges.table;
            }
            final Map<String, Object> values = new HashMap<>();
            values.put(ChangeRecord.FIELD_TABLE, table);
            return TemplateUtil.executeStrictTemplate(destinationTemplate, values);
        }

        private String extractText(final MElement input) {
            if(jsonFieldIsBytes) {
                final ByteBuffer byteBuffer = input.getAsBytes(jsonField);
                if(byteBuffer == null) {
                    return null;
                }
                final byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.asReadOnlyBuffer().get(bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return input.getAsString(jsonField);
        }
    }

    private static class SplitControlDoFn extends DoFn<MElement, MElement> {

        private final TupleTag<MElement> rowTag;
        private final TupleTag<MElement> controlTag;

        SplitControlDoFn(final TupleTag<MElement> rowTag, final TupleTag<MElement> controlTag) {
            this.rowTag = rowTag;
            this.controlTag = controlTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            if(ChangeRecord.getOp(element).isControl()) {
                c.output(controlTag, element);
            } else {
                c.output(rowTag, element);
            }
        }
    }

    private static class TruncateSequenceDoFn extends DoFn<MElement, KV<String, String>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null || !ChangeRecord.Op.TRUNCATE.equals(ChangeRecord.getOp(element))) {
                return;
            }
            c.output(KV.of(
                    element.getAsString(ChangeRecord.FIELD_TABLE),
                    element.getAsString(ChangeRecord.FIELD_SEQUENCE)));
        }
    }

    private static class MaxSequenceFn extends Combine.BinaryCombineFn<String> {

        @Override
        public String apply(final String left, final String right) {
            if(left == null) {
                return right;
            }
            if(right == null) {
                return left;
            }
            return ChangeRecord.compareSequence(left, right) >= 0 ? left : right;
        }
    }

    private static class WithTableKeysDoFn extends DoFn<MElement, KV<String, MElement>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final String key = element.getAsString(ChangeRecord.FIELD_TABLE)
                    + "#" + element.getAsString(ChangeRecord.FIELD_KEYS);
            c.output(KV.of(key, element));
        }
    }

    private static class SelectLatestDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {

        private final PCollectionView<Map<String, String>> truncates;

        SelectLatestDoFn(final PCollectionView<Map<String, String>> truncates) {
            this.truncates = truncates;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final KV<String, Iterable<MElement>> kv = c.element();
            if(kv == null || kv.getValue() == null) {
                return;
            }
            final MElement latest = ChangeRecord.latest(kv.getValue());
            if(latest == null) {
                return;
            }
            final String truncateSequence = c.sideInput(truncates).get(latest.getAsString(ChangeRecord.FIELD_TABLE));
            if(truncateSequence != null
                    && ChangeRecord.compareSequence(latest.getAsString(ChangeRecord.FIELD_SEQUENCE), truncateSequence) < 0) {
                return; // superseded by a TRUNCATE of the table
            }
            c.output(latest);
        }
    }

}
