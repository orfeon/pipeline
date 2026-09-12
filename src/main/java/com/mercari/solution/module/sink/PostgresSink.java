package com.mercari.solution.module.sink;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.domain.db.PostgresUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.cdc.ChangeRecord;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import com.zaxxer.hikari.HikariDataSource;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Sink module writing records to PostgreSQL (or compatible) databases with
 * {@code COPY ... FROM STDIN (FORMAT BINARY)}.
 *
 * Rows are encoded into a worker-memory COPY buffer as they arrive and flushed in one
 * transaction per batch ({@code batchSize} rows / {@code maxBatchBytes} / a window change / the
 * end of the bundle). {@code INSERT} copies straight into the destination; the other operations
 * copy into a session-scoped temporary table ({@code LIKE destination ... ON COMMIT DELETE ROWS})
 * and apply it with one set-based statement ({@code INSERT ... ON CONFLICT}, {@code DELETE ...
 * USING}, or {@code MERGE} for row-level ops / the cdc apply mode).
 */
@Sink.Module(name="postgres")
public class PostgresSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSink.class);

    private static final Pattern SETTING_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    /** What to do with the destination table before writing. */
    public enum EmptyTable {
        none,
        truncate,
        delete
    }

    /** How the cdc apply mode reacts to a {@code TRUNCATE} control record. */
    public enum OnTruncate {
        skip,
        fail,
        apply
    }

    static class Parameters implements Serializable {

        private String url;
        private String user;
        private String password;

        private String table;
        private String op;
        private List<String> keyFields;
        private List<String> updateFields;
        private String updateCondition;
        private String opField;
        private String applyStatement;

        private Integer batchSize;
        private Long maxBatchBytes;
        private Boolean createTable;
        private JsonElement emptyTable;
        private Boolean ignoreUnknownFields;
        private Map<String, String> settings;

        private Boolean cdc;
        private String sequenceField;
        private OnTruncate onTruncate;

        // resolved from emptyTable (JsonElement is not java-serializable)
        private EmptyTable emptyTableMode;

        void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            } else if(!url.startsWith("jdbc:postgresql:")) {
                errorMessages.add("parameters.url must be jdbc:postgresql: url");
            }
            if(user != null && password == null) {
                errorMessages.add("parameters.password must not be null");
            }
            if(table == null || table.isBlank()) {
                errorMessages.add("parameters.table must not be null");
            }
            if(op != null) {
                try {
                    final PostgresUtil.WriteOp writeOp = PostgresUtil.WriteOp.valueOf(op);
                    if(PostgresUtil.WriteOp.MERGE.equals(writeOp)) {
                        errorMessages.add("parameters.op: MERGE is selected implicitly by parameters.opField or parameters.cdc");
                    }
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.op: " + op + " is not supported. supported ops: INSERT, INSERT_OR_UPDATE, INSERT_OR_DONOTHING, DELETE");
                }
            }
            final boolean cdcMode = Boolean.TRUE.equals(cdc);
            if(opField != null && op != null) {
                errorMessages.add("parameters.op must not be set together with parameters.opField (the op comes from the field)");
            }
            if(opField != null && cdcMode) {
                errorMessages.add("parameters.opField is not applicable with parameters.cdc (the op comes from the change record)");
            }
            if(cdcMode && op != null) {
                errorMessages.add("parameters.op is not applicable with parameters.cdc (the op comes from the change record)");
            }
            if(updateCondition != null && !"INSERT_OR_UPDATE".equals(op)) {
                errorMessages.add("parameters.updateCondition is only applicable with op: INSERT_OR_UPDATE");
            }
            if(updateFields != null && !(opField != null || cdcMode || "INSERT_OR_UPDATE".equals(op))) {
                errorMessages.add("parameters.updateFields is only applicable with op: INSERT_OR_UPDATE, opField or cdc");
            }
            if(applyStatement != null && cdcMode) {
                errorMessages.add("parameters.applyStatement is not applicable with parameters.cdc");
            }
            if(sequenceField != null && !cdcMode) {
                errorMessages.add("parameters.sequenceField is only applicable with parameters.cdc");
            }
            if(onTruncate != null && !cdcMode) {
                errorMessages.add("parameters.onTruncate is only applicable with parameters.cdc");
            }
            if(batchSize != null && batchSize < 0) {
                errorMessages.add("parameters.batchSize must not be negative (0 = one transaction per bundle)");
            }
            if(maxBatchBytes != null && maxBatchBytes < 1) {
                errorMessages.add("parameters.maxBatchBytes must be positive");
            }
            if(emptyTable != null) {
                try {
                    parseEmptyTable(emptyTable);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(e.getMessage());
                }
            }
            if(Boolean.TRUE.equals(createTable) && (cdcMode || TemplateUtil.isTemplateText(table))) {
                errorMessages.add("parameters.createTable requires a fixed table name (not applicable with cdc or a templated table)");
            }
            if(emptyTable != null && TemplateUtil.isTemplateText(table)) {
                errorMessages.add("parameters.emptyTable requires a fixed table name");
            }
            if(settings != null) {
                for(final Map.Entry<String, String> entry : settings.entrySet()) {
                    if(entry.getKey() == null || !SETTING_NAME.matcher(entry.getKey()).matches()) {
                        errorMessages.add("parameters.settings key: " + entry.getKey() + " is not a valid setting name");
                    }
                    if(entry.getValue() == null) {
                        errorMessages.add("parameters.settings." + entry.getKey() + " must not be null");
                    }
                }
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private static EmptyTable parseEmptyTable(final JsonElement json) {
            if(json.isJsonPrimitive() && json.getAsJsonPrimitive().isBoolean()) {
                return json.getAsBoolean() ? EmptyTable.truncate : EmptyTable.none;
            }
            if(json.isJsonPrimitive() && json.getAsJsonPrimitive().isString()) {
                try {
                    return EmptyTable.valueOf(json.getAsString());
                } catch (final IllegalArgumentException e) {
                    // fall through
                }
            }
            throw new IllegalArgumentException("parameters.emptyTable must be true, false, \"truncate\" or \"delete\": " + json);
        }

        void setDefaults() {
            if(op == null) {
                op = PostgresUtil.WriteOp.INSERT.name();
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(batchSize == null) {
                batchSize = 100_000;
            }
            if(maxBatchBytes == null) {
                maxBatchBytes = 64L * 1024 * 1024;
            }
            if(createTable == null) {
                createTable = false;
            }
            emptyTableMode = emptyTable == null ? EmptyTable.none : parseEmptyTable(emptyTable);
            emptyTable = null;
            if(ignoreUnknownFields == null) {
                ignoreUnknownFields = false;
            }
            if(settings == null) {
                settings = new HashMap<>();
            }
            if(cdc == null) {
                cdc = false;
            }
            if(onTruncate == null) {
                onTruncate = OnTruncate.skip;
            }
        }

        PostgresUtil.WriteOp writeOp() {
            if(cdc || opField != null) {
                return PostgresUtil.WriteOp.MERGE;
            }
            return PostgresUtil.WriteOp.valueOf(op);
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults();
        final PostgresUtil.Credentials credentials = PostgresUtil
                .resolveCredentials(parameters.url, parameters.user, parameters.password, inputs.getPipeline().getOptions());

        final Schema inputSchema = Union.createUnionSchema(inputs);
        if(parameters.cdc) {
            for(final String field : List.of(
                    ChangeRecord.FIELD_TABLE, ChangeRecord.FIELD_OP, ChangeRecord.FIELD_KEYS, ChangeRecord.FIELD_SEQUENCE)) {
                if(!inputSchema.hasField(field)) {
                    throw new IllegalModuleException(
                            "postgres sink module[" + getName() + "] with cdc mode requires unified change records (the cdc transform output) as input. missing field: " + field);
                }
            }
        } else if(parameters.opField != null && !inputSchema.hasField(parameters.opField)) {
            throw new IllegalModuleException(
                    "postgres sink module[" + getName() + "].opField: " + parameters.opField + " does not exist in the input schema");
        }

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final org.apache.avro.Schema inputAvroSchema = inputSchema.getAvroSchema();
        final boolean fixedTable = !parameters.cdc || !TemplateUtil.isTemplateText(parameters.table);

        // table preparation (DDL / emptying) runs once before any write; the writes wait for it
        final List<String> ddls = new ArrayList<>();
        if(fixedTable) {
            final PostgresUtil.TableId tableId = PostgresUtil.parseTableId(parameters.table);
            if(parameters.createTable) {
                final List<String> keyFields = parameters.keyFields.isEmpty() && parameters.cdc
                        ? new ArrayList<>() : parameters.keyFields;
                ddls.add(PostgresUtil.createCreateTableStatement(tableId, inputAvroSchema, keyFields));
            }
            switch (parameters.emptyTableMode) {
                case truncate -> ddls.add(PostgresUtil.createTruncateStatement(tableId));
                case delete -> ddls.add(PostgresUtil.createDeleteAllStatement(tableId));
                case none -> { }
            }
            // validate the destination at launch when it must already exist
            if(!parameters.createTable) {
                validateDestination(credentials, tableId, inputAvroSchema, parameters);
            }
        }

        final PCollection<MElement> tableReady;
        if(ddls.isEmpty()) {
            tableReady = input;
        } else {
            final PCollection<String> wait = input.getPipeline()
                    .apply("SupplyDDL", Create.of(List.of(ddls)).withCoder(ListCoder.of(StringUtf8Coder.of())))
                    .apply("PrepareTable", ParDo.of(new PrepareTableDoFn(getName(), credentials)));
            tableReady = input
                    .apply("WaitToTablePreparation", Wait.on(wait))
                    .setCoder(input.getCoder());
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};
        final PCollectionTuple outputs = tableReady
                .apply("WriteCopy", ParDo
                        .of(new WriteDoFn(
                                getName(), credentials, parameters, inputAvroSchema.toString(),
                                getFailFast(), getLoggings(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), createOutputSchema());
    }

    public static Schema createOutputSchema() {
        return Schema.builder()
                .withField(Schema.Field.of("table", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("op", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("rows", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("affectedRows", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("startedAt", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .withField(Schema.Field.of("finishedAt", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .build();
    }

    private void validateDestination(
            final PostgresUtil.Credentials credentials,
            final PostgresUtil.TableId tableId,
            final org.apache.avro.Schema inputAvroSchema,
            final Parameters parameters) {

        try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                .createDataSource(PostgresUtil.DRIVER, credentials.url(), credentials.user(), credentials.password(), true)) {
            try(final Connection connection = dataSource.getConnection()) {
                if(!PostgresUtil.tableExists(connection, tableId)) {
                    throw new IllegalModuleException("postgres sink module[" + getName() + "].table: " + tableId.qualifiedName()
                            + " does not exist. create it or set createTable: true");
                }
                final PostgresUtil.TableInfo info = PostgresUtil.getTableInfo(connection, tableId);
                connection.commit();
                final WritePlan plan = WritePlan.of(getName(), info, inputAvroSchema, parameters);
                LOG.info("postgres sink module[{}] writes table: {} op: {} columns: {} keys: {}",
                        getName(), tableId.qualifiedName(), plan.op, plan.columnNames, plan.keyColumns);
            }
        } catch (final IOException | SQLException e) {
            throw new IllegalModuleException("Failed to connect database. url: " + credentials.url(), e);
        }
    }

    /**
     * The resolved write of one destination table: the COPY column list (aligned with the input
     * avro fields, or looked up by name from change record JSON in cdc mode), the COPY statement
     * and, for staged operations, the staging table and the apply statement. Built once per
     * table per worker (and at launch for validation).
     */
    static class WritePlan {

        final PostgresUtil.TableId tableId;
        final PostgresUtil.WriteOp op;
        // destination columns written by COPY (without the staging op column)
        final List<PostgresUtil.Column> columns;
        final List<String> columnNames;
        // input avro fields aligned with columns (null entries never happen: unmatched fields are skipped or rejected)
        final List<org.apache.avro.Schema.Field> fields;
        final List<String> keyColumns;
        final boolean staged;
        // all COPY columns: columns plus the op column in MERGE mode
        final List<PostgresUtil.Column> copyColumns;
        final String stagingTable;
        final String copySql;
        final String applySql;
        final int opFieldPos;
        final int sequenceColumnIndex;

        private WritePlan(
                final PostgresUtil.TableId tableId,
                final PostgresUtil.WriteOp op,
                final List<PostgresUtil.Column> columns,
                final List<org.apache.avro.Schema.Field> fields,
                final List<String> keyColumns,
                final boolean staged,
                final String stagingTable,
                final String copySql,
                final String applySql,
                final int opFieldPos,
                final int sequenceColumnIndex) {

            this.tableId = tableId;
            this.op = op;
            this.columns = columns;
            this.columnNames = columns.stream().map(c -> c.name).toList();
            this.fields = fields;
            this.keyColumns = keyColumns;
            this.staged = staged;
            final List<PostgresUtil.Column> copyColumns = new ArrayList<>(columns);
            if(PostgresUtil.WriteOp.MERGE.equals(op)) {
                copyColumns.add(new PostgresUtil.Column(PostgresUtil.STAGING_OP_COLUMN, PostgresUtil.ColumnType.TEXT));
            }
            this.copyColumns = copyColumns;
            this.stagingTable = stagingTable;
            this.copySql = copySql;
            this.applySql = applySql;
            this.opFieldPos = opFieldPos;
            this.sequenceColumnIndex = sequenceColumnIndex;
        }

        static WritePlan of(
                final String name,
                final PostgresUtil.TableInfo info,
                final org.apache.avro.Schema inputSchema,
                final Parameters parameters) {

            final String prefix = "postgres sink module[" + name + "] table: " + info.tableId().qualifiedName() + " ";
            final PostgresUtil.WriteOp op = parameters.writeOp();

            final List<PostgresUtil.Column> columns = new ArrayList<>();
            final List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
            int opFieldPos = -1;
            if(parameters.cdc) {
                for(final PostgresUtil.Column column : info.columns()) {
                    if(!info.generatedColumns().contains(column.name)) {
                        columns.add(column);
                        fields.add(null);
                    }
                }
            } else {
                for(final org.apache.avro.Schema.Field field : inputSchema.getFields()) {
                    if(field.name().equals(parameters.opField)) {
                        opFieldPos = field.pos();
                        continue;
                    }
                    PostgresUtil.Column column = info.getColumn(field.name());
                    if(column == null) {
                        column = info.findColumnIgnoreCase(field.name());
                    }
                    if(column == null) {
                        if(parameters.ignoreUnknownFields) {
                            LOG.info("{}ignores input field: {} (no such column)", prefix, field.name());
                            continue;
                        }
                        throw new IllegalModuleException(prefix + "has no column for input field: " + field.name()
                                + ". table columns: " + info.columns().stream().map(c -> c.name).toList()
                                + " (set ignoreUnknownFields: true to skip it)");
                    }
                    if(info.generatedColumns().contains(column.name)) {
                        LOG.info("{}skips generated column: {}", prefix, column.name);
                        continue;
                    }
                    columns.add(column);
                    fields.add(field);
                }
            }

            final List<String> keyColumns = new ArrayList<>();
            if(!PostgresUtil.WriteOp.INSERT.equals(op)) {
                final List<String> requested = parameters.keyFields.isEmpty() ? info.primaryKey() : parameters.keyFields;
                if(requested.isEmpty()) {
                    throw new IllegalModuleException(prefix + "has no primary key. set keyFields for op: " + parameters.op);
                }
                for(final String key : requested) {
                    final PostgresUtil.Column column = resolveColumn(columns, key);
                    if(column == null) {
                        throw new IllegalModuleException(prefix + "key column: " + key + " is not among the written columns: "
                                + columns.stream().map(c -> c.name).toList());
                    }
                    keyColumns.add(column.name);
                }
            }
            if(PostgresUtil.WriteOp.DELETE.equals(op)) {
                // only the keys are staged for a delete
                final List<PostgresUtil.Column> keyOnly = new ArrayList<>();
                final List<org.apache.avro.Schema.Field> keyFields = new ArrayList<>();
                for(int i = 0; i < columns.size(); i++) {
                    if(keyColumns.contains(columns.get(i).name)) {
                        keyOnly.add(columns.get(i));
                        keyFields.add(fields.get(i));
                    }
                }
                columns.clear();
                columns.addAll(keyOnly);
                fields.clear();
                fields.addAll(keyFields);
            }

            final List<String> updateColumns = new ArrayList<>();
            if(PostgresUtil.WriteOp.INSERT_OR_UPDATE.equals(op) || PostgresUtil.WriteOp.MERGE.equals(op)) {
                if(parameters.updateFields != null) {
                    for(final String updateField : parameters.updateFields) {
                        final PostgresUtil.Column column = resolveColumn(columns, updateField);
                        if(column == null) {
                            throw new IllegalModuleException(prefix + "updateFields column: " + updateField + " is not among the written columns: "
                                    + columns.stream().map(c -> c.name).toList());
                        }
                        if(keyColumns.contains(column.name)) {
                            throw new IllegalModuleException(prefix + "updateFields must not contain key column: " + column.name);
                        }
                        updateColumns.add(column.name);
                    }
                } else {
                    for(final PostgresUtil.Column column : columns) {
                        if(!keyColumns.contains(column.name)) {
                            updateColumns.add(column.name);
                        }
                    }
                }
            }

            if((PostgresUtil.WriteOp.INSERT_OR_UPDATE.equals(op) || PostgresUtil.WriteOp.INSERT_OR_DONOTHING.equals(op))
                    && parameters.applyStatement == null
                    && !info.hasUniqueIndex(keyColumns)) {
                throw new IllegalModuleException(prefix + "has no unique index on the key columns: " + keyColumns
                        + " (required by ON CONFLICT). create one (CREATE UNIQUE INDEX ... ON ... (" + String.join(", ", keyColumns)
                        + ")), or specify applyStatement. unique indexes: " + info.uniqueIndexes());
            }
            if(PostgresUtil.WriteOp.MERGE.equals(op) && info.serverVersion() < 150000) {
                throw new IllegalModuleException(prefix + "opField / cdc apply requires PostgreSQL 15 or later (MERGE). server_version_num: " + info.serverVersion());
            }

            int sequenceColumnIndex = -1;
            if(parameters.sequenceField != null) {
                final PostgresUtil.Column column = resolveColumn(columns, parameters.sequenceField);
                if(column == null) {
                    throw new IllegalModuleException(prefix + "sequenceField column: " + parameters.sequenceField + " does not exist");
                }
                if(!PostgresUtil.ColumnType.TEXT.equals(column.type) && !PostgresUtil.ColumnType.VARCHAR.equals(column.type)) {
                    throw new IllegalModuleException(prefix + "sequenceField column: " + column.name + " must be a text or varchar column");
                }
                sequenceColumnIndex = columns.indexOf(column);
            }

            final boolean staged = op.isStaged() || parameters.applyStatement != null;
            final List<String> columnNames = columns.stream().map(c -> c.name).toList();
            final List<String> copyColumnNames = new ArrayList<>(columnNames);
            if(PostgresUtil.WriteOp.MERGE.equals(op)) {
                copyColumnNames.add(PostgresUtil.STAGING_OP_COLUMN);
            }
            final String stagingTable = staged ? PostgresUtil.createStagingTableName(info.tableId(), columnNames, op) : null;
            final String copySql = PostgresUtil.createCopyInStatement(
                    staged ? PostgresUtil.quoteIdentifier(stagingTable) : info.tableId().quotedName(),
                    copyColumnNames.stream().map(PostgresUtil::quoteIdentifier).toList());
            final String applySql;
            if(!staged) {
                applySql = null;
            } else if(parameters.applyStatement != null) {
                final Map<String, Object> model = new HashMap<>();
                model.put("target", info.tableId().quotedName());
                model.put("staging", PostgresUtil.quoteIdentifier(stagingTable));
                model.put("columns", String.join(", ", columnNames.stream().map(PostgresUtil::quoteIdentifier).toList()));
                model.put("keyColumns", String.join(", ", keyColumns.stream().map(PostgresUtil::quoteIdentifier).toList()));
                model.put("updateColumns", String.join(", ", updateColumns.stream().map(PostgresUtil::quoteIdentifier).toList()));
                applySql = TemplateUtil.executeStrictTemplate(parameters.applyStatement, model);
            } else {
                applySql = PostgresUtil.createApplyStatement(new PostgresUtil.ApplySpec(
                        op, info.tableId(), stagingTable, columnNames, keyColumns, updateColumns,
                        parameters.updateCondition,
                        sequenceColumnIndex < 0 ? null : columns.get(sequenceColumnIndex).name));
            }
            return new WritePlan(info.tableId(), op, columns, fields, keyColumns, staged, stagingTable, copySql, applySql, opFieldPos, sequenceColumnIndex);
        }

        private static PostgresUtil.Column resolveColumn(final List<PostgresUtil.Column> columns, final String name) {
            for(final PostgresUtil.Column column : columns) {
                if(column.name.equals(name)) {
                    return column;
                }
            }
            PostgresUtil.Column found = null;
            for(final PostgresUtil.Column column : columns) {
                if(column.name.equalsIgnoreCase(name)) {
                    if(found != null) {
                        return null;
                    }
                    found = column;
                }
            }
            return found;
        }

        int columnIndex(final String name) {
            return columnNames.indexOf(name);
        }
    }

    /**
     * The change record sequence ({@code hex/hex/...}) left-padded per section so that the
     * C-collation text comparison in the MERGE guard orders it like
     * {@link ChangeRecord#compareSequence}.
     */
    static String padSequence(final String sequence) {
        final String[] sections = sequence.split("/");
        final StringBuilder sb = new StringBuilder();
        for(final String section : sections) {
            if(!sb.isEmpty()) {
                sb.append('/');
            }
            sb.append("0".repeat(Math.max(0, 16 - section.length()))).append(section);
        }
        return sb.toString();
    }

    /** Runs the table preparation statements (CREATE TABLE / TRUNCATE / DELETE) once. */
    private static class PrepareTableDoFn extends DoFn<List<String>, String> {

        private final String name;
        private final PostgresUtil.Credentials credentials;

        PrepareTableDoFn(final String name, final PostgresUtil.Credentials credentials) {
            this.name = name;
            this.credentials = credentials;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            final List<String> ddls = c.element();
            if(ddls == null || ddls.isEmpty()) {
                c.output("ok");
                return;
            }
            try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                    .createDataSource(PostgresUtil.DRIVER, credentials.url(), credentials.user(), credentials.password())) {
                try(final Connection connection = dataSource.getConnection();
                    final Statement statement = connection.createStatement()) {
                    for(final String ddl : ddls) {
                        LOG.info("postgres sink module[{}] executes: {}", name, ddl);
                        statement.executeUpdate(ddl);
                    }
                    connection.commit();
                }
            }
            c.output("ok");
        }
    }

    /** A byte buffer whose contents can be streamed without copying. */
    private static class CopyBuffer extends ByteArrayOutputStream {

        CopyBuffer(final int size) {
            super(size);
        }

        InputStream toInputStream() {
            return new ByteArrayInputStream(buf, 0, count);
        }
    }

    /** The COPY payload of one destination table accumulated for the next transaction. */
    private static class Batch {

        private final WritePlan plan;
        private final CopyBuffer buffer;
        private final DataOutputStream output;
        private long rows;

        Batch(final WritePlan plan) throws IOException {
            this.plan = plan;
            this.buffer = new CopyBuffer(64 * 1024);
            this.output = new DataOutputStream(buffer);
            PostgresUtil.writeHeader(output);
        }
    }

    /** The batches of one window (one transaction per flush) and the event time of their control records. */
    private static class WindowBatches {

        private final Map<String, Batch> tables = new LinkedHashMap<>();
        private Instant maxTimestamp;
    }

    /** A control record waiting for the end of the bundle, where it can be emitted into its own window. */
    private record PendingOutput(MElement element, Instant timestamp, BoundedWindow window) { }

    private static class WriteDoFn extends DoFn<MElement, MElement> {

        private final String name;
        private final PostgresUtil.Credentials credentials;
        private final Parameters parameters;
        private final String inputSchemaString;
        private final boolean failFast;
        private final Map<String, Logging> logs;
        private final TupleTag<BadRecord> failureTag;

        private transient org.apache.avro.Schema inputSchema;
        private transient HikariDataSource dataSource;
        private transient Template tableTemplate;
        private transient Map<String, WritePlan> plans;
        // batches are kept per window: a transaction never spans windows
        private transient Map<BoundedWindow, WindowBatches> windows;
        private transient List<PendingOutput> pendingOutputs;
        private transient CopyBuffer rowBuffer;
        private transient DataOutputStream rowOutput;
        private transient long bufferedRows;
        private transient long bufferedBytes;
        private transient Set<String> reportedUnknownColumns;
        private transient Counter controlCounter;
        private transient Counter transactionCounter;

        WriteDoFn(
                final String name,
                final PostgresUtil.Credentials credentials,
                final Parameters parameters,
                final String inputSchemaString,
                final boolean failFast,
                final List<Logging> loggings,
                final TupleTag<BadRecord> failureTag) {

            this.name = name;
            this.credentials = credentials;
            this.parameters = parameters;
            this.inputSchemaString = inputSchemaString;
            this.failFast = failFast;
            this.logs = Logging.map(loggings);
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.inputSchema = AvroSchemaUtil.convertSchema(inputSchemaString);
            this.dataSource = PostgresUtil.acquirePool(credentials, false, 10);
            this.plans = new HashMap<>();
            this.windows = new LinkedHashMap<>();
            this.pendingOutputs = new ArrayList<>();
            this.rowBuffer = new CopyBuffer(4096);
            this.rowOutput = new DataOutputStream(rowBuffer);
            this.reportedUnknownColumns = new HashSet<>();
            this.controlCounter = Metrics.counter(name, "postgres_sink_cdc_control_records");
            this.transactionCounter = Metrics.counter(name, "postgres_sink_transactions");
            if(parameters.cdc && TemplateUtil.isTemplateText(parameters.table)) {
                this.tableTemplate = TemplateUtil.createStrictTemplate("postgresSinkTable", parameters.table);
            }
        }

        @Teardown
        public void teardown() {
            if(dataSource != null) {
                dataSource = null;
                PostgresUtil.releasePool(credentials, false);
            }
        }

        @StartBundle
        public void startBundle() {
            windows.clear();
            pendingOutputs.clear();
            bufferedRows = 0;
            bufferedBytes = 0;
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) throws IOException {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            Logging.log(LOG, logs, "input", element);
            try {
                final WindowBatches batches = windows.computeIfAbsent(window, w -> new WindowBatches());
                if(batches.maxTimestamp == null || c.timestamp().isAfter(batches.maxTimestamp)) {
                    batches.maxTimestamp = c.timestamp();
                }
                if(parameters.cdc) {
                    bufferChangeRecord(element, batches);
                } else {
                    bufferElement(element, batches);
                }
            } catch (final Throwable e) {
                c.output(failureTag, processError("Failed to encode record for postgres sink: " + name, element, e, failFast));
            }

            if((parameters.batchSize > 0 && bufferedRows >= parameters.batchSize) || bufferedBytes >= parameters.maxBatchBytes) {
                flushAll();
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) throws IOException {
            flushAll();
            for(final PendingOutput output : pendingOutputs) {
                c.output(output.element(), output.timestamp(), output.window());
            }
            pendingOutputs.clear();
        }

        private WritePlan plan(final String table) {
            WritePlan plan = plans.get(table);
            if(plan != null) {
                return plan;
            }
            final PostgresUtil.TableId tableId = PostgresUtil.parseTableId(table);
            try(final Connection connection = dataSource.getConnection()) {
                if(!PostgresUtil.tableExists(connection, tableId)) {
                    throw new IllegalStateException("postgres sink module[" + name + "] table: " + tableId.qualifiedName() + " does not exist");
                }
                final PostgresUtil.TableInfo info = PostgresUtil.getTableInfo(connection, tableId);
                connection.commit();
                plan = WritePlan.of(name, info, inputSchema, parameters);
            } catch (final SQLException e) {
                throw new IllegalStateException("Failed to resolve postgres table: " + table, e);
            }
            LOG.info("postgres sink module[{}] resolved table: {} op: {} columns: {} keys: {} copy: [{}] apply: [{}]",
                    name, tableId.qualifiedName(), plan.op, plan.columnNames, plan.keyColumns, plan.copySql, plan.applySql);
            plans.put(table, plan);
            return plan;
        }

        private void bufferElement(final MElement element, final WindowBatches batches) throws IOException {
            final WritePlan plan = plan(parameters.table);
            final GenericRecord record = ElementToAvroConverter.convert(inputSchema, element);
            rowBuffer.reset();
            if(PostgresUtil.WriteOp.MERGE.equals(plan.op)) {
                final Object[] values = new Object[plan.copyColumns.size()];
                for(int i = 0; i < plan.columns.size(); i++) {
                    final org.apache.avro.Schema.Field field = plan.fields.get(i);
                    values[i] = field == null ? null : record.get(field.pos());
                }
                values[plan.columns.size()] = toStagingOp(record.get(plan.opFieldPos));
                writeTypedValues(plan, values);
            } else {
                PostgresUtil.write(rowOutput, plan.columns, plan.fields, record);
            }
            append(plan, batches);
        }

        /** Encodes values with the avro field schemas where the plan has them (decimal scale, millis logical types). */
        private void writeTypedValues(final WritePlan plan, final Object[] values) throws IOException {
            rowOutput.writeShort(plan.copyColumns.size());
            for(int i = 0; i < plan.copyColumns.size(); i++) {
                final Object value = values[i];
                if(value == null) {
                    rowOutput.writeInt(-1);
                    continue;
                }
                final org.apache.avro.Schema.Field field = i < plan.fields.size() ? plan.fields.get(i) : null;
                PostgresUtil.encodeValue(rowOutput, plan.copyColumns.get(i),
                        field == null ? null : AvroSchemaUtil.unnestUnion(field.schema()), value);
            }
        }

        private static String toStagingOp(final Object value) {
            if(value == null) {
                throw new IllegalArgumentException("opField value must not be null");
            }
            final String op = value.toString().trim().toUpperCase();
            return switch (op) {
                case "DELETE", "D" -> PostgresUtil.STAGING_OP_DELETE;
                case "INSERT", "UPDATE", "UPSERT", "SNAPSHOT", "I", "U", "C", "R" -> PostgresUtil.STAGING_OP_UPSERT;
                default -> throw new IllegalArgumentException("Unsupported opField value: " + value + " (expected INSERT, UPDATE or DELETE)");
            };
        }

        private void append(final WritePlan plan, final WindowBatches batches) throws IOException {
            Batch batch = batches.tables.get(plan.tableId.qualifiedName());
            if(batch == null) {
                batch = new Batch(plan);
                batches.tables.put(plan.tableId.qualifiedName(), batch);
            }
            rowOutput.flush();
            rowBuffer.writeTo(batch.output);
            batch.rows++;
            bufferedRows++;
            bufferedBytes += rowBuffer.size();
        }

        private void bufferChangeRecord(final MElement element, final WindowBatches batches) throws IOException {
            final Map<String, Object> envelope = element.asPrimitiveMap();
            final ChangeRecord.Op op = ChangeRecord.getOp(envelope.get(ChangeRecord.FIELD_OP));
            final String sourceTable = String.valueOf(envelope.get(ChangeRecord.FIELD_TABLE));
            final String table = tableTemplate == null
                    ? parameters.table
                    : TemplateUtil.executeStrictTemplate(tableTemplate, Map.of(ChangeRecord.FIELD_TABLE, sourceTable));
            if(op.isControl()) {
                controlCounter.inc();
                if(ChangeRecord.Op.TRUNCATE.equals(op)) {
                    switch (parameters.onTruncate) {
                        case fail -> throw new IllegalStateException(
                                "postgres sink module[" + name + "] received TRUNCATE of table: " + sourceTable + " (onTruncate: fail)");
                        case apply -> {
                            // the rows buffered so far precede the truncate
                            flushAll();
                            final WritePlan plan = plan(table);
                            execute(PostgresUtil.createTruncateStatement(plan.tableId));
                            LOG.info("postgres sink module[{}] truncated table: {} (cdc TRUNCATE of {})", name, plan.tableId.qualifiedName(), sourceTable);
                        }
                        case skip -> LOG.info("postgres sink module[{}] skips cdc control record: {} of table: {}", name, op, sourceTable);
                    }
                } else {
                    LOG.info("postgres sink module[{}] skips cdc control record: {} of table: {}", name, op, sourceTable);
                }
                return;
            }

            final WritePlan plan = plan(table);
            final JsonObject keys = parseObject(envelope.get(ChangeRecord.FIELD_KEYS));
            if(keys == null || keys.isEmpty()) {
                throw new IllegalArgumentException("change record requires keys to be applied to postgres table: " + plan.tableId.qualifiedName());
            }
            final JsonObject after = ChangeRecord.Op.DELETE.equals(op) ? null : parseObject(envelope.get(ChangeRecord.FIELD_AFTER));
            final Object[] values = new Object[plan.copyColumns.size()];
            for(int i = 0; i < plan.columns.size(); i++) {
                final PostgresUtil.Column column = plan.columns.get(i);
                if(i == plan.sequenceColumnIndex) {
                    values[i] = padSequence(String.valueOf(envelope.get(ChangeRecord.FIELD_SEQUENCE)));
                    continue;
                }
                final JsonElement json;
                if(keys.has(column.name)) {
                    json = keys.get(column.name);
                } else if(after != null && after.has(column.name)) {
                    json = after.get(column.name);
                } else {
                    json = null;
                }
                values[i] = PostgresUtil.fromJsonValue(column, json);
            }
            reportUnknownColumns(plan, keys);
            if(after != null) {
                reportUnknownColumns(plan, after);
            }
            values[plan.columns.size()] = ChangeRecord.Op.DELETE.equals(op) ? PostgresUtil.STAGING_OP_DELETE : PostgresUtil.STAGING_OP_UPSERT;
            rowBuffer.reset();
            PostgresUtil.writeValues(rowOutput, plan.copyColumns, values);
            append(plan, batches);
        }

        private void reportUnknownColumns(final WritePlan plan, final JsonObject json) {
            for(final String key : json.keySet()) {
                if(plan.columnIndex(key) < 0 && reportedUnknownColumns.add(plan.tableId.qualifiedName() + "." + key)) {
                    LOG.warn("postgres sink module[{}] change record column: {} does not exist on table: {}, the value is ignored",
                            name, key, plan.tableId.qualifiedName());
                }
            }
        }

        private static JsonObject parseObject(final Object value) {
            if(value == null) {
                return null;
            }
            final String json = value.toString();
            if(json.isBlank()) {
                return null;
            }
            final JsonElement parsed = JsonParser.parseString(json);
            return parsed.isJsonObject() ? parsed.getAsJsonObject() : null;
        }

        private void execute(final String sql) {
            try(final Connection connection = dataSource.getConnection();
                final Statement statement = connection.createStatement()) {
                try {
                    statement.executeUpdate(sql);
                    connection.commit();
                } catch (final SQLException e) {
                    connection.rollback();
                    throw e;
                }
            } catch (final SQLException e) {
                throw new IllegalStateException("Failed to execute: " + sql, e);
            }
        }

        /** Writes every buffered window (one transaction each); the control records wait for the end of the bundle. */
        private void flushAll() throws IOException {
            try {
                for(final Map.Entry<BoundedWindow, WindowBatches> entry : windows.entrySet()) {
                    flush(entry.getKey(), entry.getValue());
                }
            } finally {
                windows.clear();
                bufferedRows = 0;
                bufferedBytes = 0;
            }
        }

        /** Writes the batches of one window in one transaction and queues a control record per table. */
        private void flush(final BoundedWindow window, final WindowBatches batches) throws IOException {
            if(batches.tables.isEmpty()) {
                return;
            }
            final Instant startedAt = Instant.now();
            final List<Map<String, Object>> results = new ArrayList<>();
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                final PGConnection pgConnection = connection.unwrap(PGConnection.class);
                try(final Statement statement = connection.createStatement()) {
                    for(final Map.Entry<String, String> setting : parameters.settings.entrySet()) {
                        statement.execute("SET LOCAL " + setting.getKey() + " = " + PostgresUtil.quoteLiteral(setting.getValue()));
                    }
                    for(final Batch batch : batches.tables.values()) {
                        final WritePlan plan = batch.plan;
                        PostgresUtil.writeTrailer(batch.output);
                        batch.output.flush();
                        if(plan.staged) {
                            statement.execute(PostgresUtil.createStagingTableStatement(
                                    plan.stagingTable, plan.tableId, PostgresUtil.WriteOp.MERGE.equals(plan.op)));
                        }
                        final long copied = pgConnection.getCopyAPI().copyIn(plan.copySql, batch.buffer.toInputStream());
                        final long affected = plan.staged ? statement.executeUpdate(plan.applySql) : copied;
                        final Map<String, Object> result = new HashMap<>();
                        result.put("table", plan.tableId.qualifiedName());
                        result.put("op", plan.op.name());
                        result.put("rows", copied);
                        result.put("affectedRows", affected);
                        results.add(result);
                    }
                }
                connection.commit();
                transactionCounter.inc();
            } catch (final Throwable e) {
                if(connection != null) {
                    try {
                        connection.rollback();
                    } catch (final SQLException rollbackException) {
                        e.addSuppressed(rollbackException);
                    }
                    // the session may be in an unknown state: drop it from the pool
                    dataSource.evictConnection(connection);
                }
                if(e instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new IllegalStateException("Failed to write postgres batch: " + name, e);
            } finally {
                if(connection != null) {
                    try {
                        connection.close();
                    } catch (final SQLException e) {
                        LOG.warn("Failed to close connection", e);
                    }
                }
            }

            final Instant finishedAt = Instant.now();
            final long millis = finishedAt.getMillis() - startedAt.getMillis();
            for(final Map<String, Object> result : results) {
                LOG.info("postgres sink module[{}] wrote table: {} op: {} rows: {} affected: {} in {} ms",
                        name, result.get("table"), result.get("op"), result.get("rows"), result.get("affectedRows"), millis);
                final MElement output = MElement.builder()
                        .withString("table", (String) result.get("table"))
                        .withString("op", (String) result.get("op"))
                        .withInt64("rows", (Long) result.get("rows"))
                        .withInt64("affectedRows", (Long) result.get("affectedRows"))
                        .withTimestamp("startedAt", startedAt)
                        .withTimestamp("finishedAt", finishedAt)
                        .withEventTime(batches.maxTimestamp)
                        .build();
                Logging.log(LOG, logs, "output", output);
                pendingOutputs.add(new PendingOutput(output, batches.maxTimestamp, window));
            }
        }
    }

}
