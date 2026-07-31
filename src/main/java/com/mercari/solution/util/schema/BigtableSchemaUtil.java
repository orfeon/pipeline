package com.mercari.solution.util.schema;

import com.google.bigtable.v2.*;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.Value;
import com.google.cloud.ByteArray;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.domain.file.ResourceUtil;
import freemarker.template.Template;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.values.KV;
import com.mercari.solution.util.HBaseBytes;
import org.joda.time.Instant;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class BigtableSchemaUtil {

    private static final String RESOURCE_CDC_AVRO_SCHEMA_PATH = "/schema/avro/bigtable_cdc.avsc";
    private static final String RESOURCE_RUNTIME_CDC_AVRO_SCHEMA_PATH = "/template/MPipeline/resources/schema/avro/bigtable_cdc.avsc";

    public enum Format {
        bytes,
        avro,
        text,
        avromap
    }

    public enum MutationOp implements Serializable {
        SET_CELL,
        ADD_TO_CELL,
        MERGE_TO_CELL,
        DELETE_FROM_COLUMN,
        DELETE_FROM_FAMILY,
        DELETE_FROM_ROW
    }

    public enum TimestampType implements Serializable {
        server,
        event,
        current,
        field,
        fixed,
        zero
    }

    public enum CellType implements Serializable {
        all,
        first,
        last
    }

    /**
     * Cell-level encoding declaration (schema-redesign.md P4): the shared encoding/reference
     * vocabulary applied at bigtable's cell granularity, at any cascade level
     * (parameters top level, column family, or qualifier). {@code format} is the new spelling
     * of the legacy {@code format} key; {@code reference} supplies the Avro schema document
     * (uri or inline) for avro-encoded cells, replacing the need to declare nested fields.
     */
    public static class CellEncoding implements Serializable {

        private Format format;
        private Schema.Reference reference;

        public Format getFormat() {
            return format;
        }

        public Schema.Reference getReference() {
            return reference;
        }

        public List<String> validate(final String path) {
            final List<String> errorMessages = new ArrayList<>();
            if(reference != null) {
                if(!Format.avro.equals(format) && !Format.avromap.equals(format)) {
                    errorMessages.add(path + ".encoding.reference requires encoding.format avro");
                }
                if(reference.getUri() == null && reference.getInline() == null) {
                    errorMessages.add(path + ".encoding.reference requires uri or inline");
                }
            }
            return errorMessages;
        }

        public List<String> validateConflict(final Format declaredFormat, final String path) {
            final List<String> errorMessages = new ArrayList<>(validate(path));
            if(format != null && declaredFormat != null && !format.equals(declaredFormat)) {
                errorMessages.add(path + ".format: " + declaredFormat + " conflicts with " + path + ".encoding.format: " + format);
            }
            return errorMessages;
        }

    }

    public enum ModType {

        SET_CELL(0),
        DELETE_FAMILY(1),
        DELETE_CELLS(2),
        UNKNOWN(3);

        private final int id;

        ModType(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

    public static class ColumnFamilyProperties implements Serializable {

        private String family;
        private List<ColumnQualifierProperties> qualifiers;
        private Format format;
        private CellEncoding encoding;

        // for sink
        private String mutationOp;
        private TimestampType timestampType;
        private String timestampField;
        private String timestampValue;

        // for source
        private CellType cellType;

        private transient Template templateFamily;
        private transient Template templateMutationOp;

        public List<String> validate(int i) {
            final List<String> errorMessages = new ArrayList<>();
            if(family == null) {
                errorMessages.add("parameters.columns[" + i + "].family must not be null");
            }
            if(encoding != null) {
                errorMessages.addAll(encoding.validateConflict(format, "parameters.columns[" + i + "]"));
            }
            // qualifiers may be absent or empty: setDefaults derives them from the input schema fields
            if(qualifiers != null) {
                for(int j=0; j<qualifiers.size(); j++) {
                    errorMessages.addAll(qualifiers.get(j).validate(i, j));
                }
            }
            return errorMessages;
        }

        // for source read and transform read
        public void setDefaults(
                final Format defaultFormat,
                final CellType cellType) {

            setDefaults(defaultFormat, null, null, null, null, cellType, null);
        }

        // for sink write cell
        public void setDefaults(
                final Format defaultFormat,
                final String defaultMutationOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue,
                final List<Schema.Field> fields) {

            setDefaults(defaultFormat, defaultMutationOp,
                    defaultTimestampType, defaultTimestampField, defaultTimestampValue,
                    null, fields);
        }

        // for read cell
        private void setDefaults(
                final Format defaultFormat,
                final String defaultMutationOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue,
                final CellType defaultCellType,
                final List<Schema.Field> fields) {

            if(format == null && encoding != null && encoding.format != null) {
                format = encoding.format;
            }
            if(format == null) {
                format = defaultFormat;
            }
            if(mutationOp == null) {
                mutationOp = Optional
                        .ofNullable(defaultMutationOp)
                        .orElse(MutationOp.SET_CELL.name());
            }
            if(timestampType == null) {
                timestampType = Optional
                        .ofNullable(defaultTimestampType)
                        .orElse(TimestampType.server);
            }
            if(timestampField == null) {
                timestampField = defaultTimestampField;
            }
            if(timestampValue == null) {
                timestampValue = defaultTimestampValue;
            }
            if(cellType == null) {
                cellType = Optional.ofNullable(defaultCellType).orElse(CellType.last);
            }
            if(qualifiers == null || qualifiers.isEmpty()) {
                qualifiers = new ArrayList<>();
                if(fields != null && !TemplateUtil.isTemplateText(mutationOp)) {
                    switch (MutationOp.valueOf(mutationOp)) {
                        case SET_CELL, DELETE_FROM_COLUMN -> {
                            // derive qualifiers from the input schema fields (qualifier name = field name)
                            for(final Schema.Field field : fields) {
                                final ColumnQualifierProperties qualifier = ColumnQualifierProperties.of(field);
                                qualifiers.add(qualifier);
                            }
                        }
                    }
                }
            }
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                qualifier.setDefaults(format, mutationOp, timestampType, timestampField, timestampValue);
            }
        }

        public List<String> extractValueArgs() {
            final List<String> valueArgs = new ArrayList<>();
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                valueArgs.add(qualifier.field);
                if(qualifier.timestampField != null) {
                    valueArgs.add(qualifier.timestampField);
                }
            }
            return valueArgs;
        }

        public List<String> extractTemplateArgs(final Schema inputSchema) {
            final List<String> templateArgs = TemplateUtil.extractTemplateArgs(family, inputSchema);
            if(TemplateUtil.isTemplateText(mutationOp)) {
                templateArgs.addAll(TemplateUtil.extractTemplateArgs(mutationOp, inputSchema));
            }
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                templateArgs.addAll(qualifier.extractTemplateArgs(inputSchema));
            }
            return templateArgs;
        }

        public void setupSource() {
            this.templateFamily = TemplateUtil.createStrictTemplate("templateColumnFamily", family);
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                qualifier.setupSource();
            }
        }

        public void setupSink() {
            this.templateFamily = TemplateUtil.createStrictTemplate("templateColumnFamily", family);
            if(TemplateUtil.isTemplateText(mutationOp)) {
                this.templateMutationOp = TemplateUtil.createStrictTemplate("templateMutationOp", mutationOp);
            }
            for(final ColumnQualifierProperties qualifier : qualifiers) {
                qualifier.setupSink();
            }
        }

        private List<Mutation> toMutation(
                final Map<String, Object> primitiveValues,
                final Map<String, Object> standardValues,
                final Instant timestamp) {

            final String cf = TemplateUtil.executeStrictTemplate(templateFamily, standardValues);
            final MutationOp resolvedMutationOp = resolveMutationOp(mutationOp, templateMutationOp, standardValues);
            final List<Mutation> mutations = new ArrayList<>();
            if(MutationOp.DELETE_FROM_FAMILY.equals(resolvedMutationOp)) {
                final Mutation mutation = Mutation.newBuilder()
                        .setDeleteFromFamily(Mutation.DeleteFromFamily.newBuilder()
                                .setFamilyName(cf)
                                .build())
                        .build();
                mutations.add(mutation);
            } else {
                for(final ColumnQualifierProperties qualifier : qualifiers) {
                    final Mutation mutation = qualifier.toMutation(cf, primitiveValues, standardValues, timestamp);
                    if(mutation == null) {
                        continue;
                    }
                    mutations.add(mutation);
                }
            }
            return mutations;
        }

        private Map<String, Object> toElement(final Family family) {
            final Map<String, Object> primitiveValues = new HashMap<>();
            for(final Column column : family.getColumnsList()) {
                for(final ColumnQualifierProperties qualifierProperty : qualifiers) {
                    if(qualifierProperty.name.equals(column.getQualifier().toStringUtf8())) {
                        final List<Object> values = qualifierProperty.toPrimitiveValues(column);
                        if(values.isEmpty()) {
                            continue;
                        }
                        final Object cellValue = switch (cellType) {
                            case all -> values;
                            case last -> values.getFirst();
                            case first -> values.getLast();
                        };
                        primitiveValues.put(qualifierProperty.field, cellValue);
                        break;
                    }
                }
            }
            return primitiveValues;
        }

        @Override
        public String toString() {
            final String qualifiersString;
            if(qualifiers != null) {
                qualifiersString = qualifiers.stream().map(ColumnQualifierProperties::toString).collect(Collectors.joining(","));
            } else {
                qualifiersString = null;
            }
            return String.format("family: %s, qualifiers: %s", family, qualifiersString);
        }

    }

    public static class ColumnQualifierProperties implements Schema.IField {

        private String name;
        private String field;
        private Format format;
        private CellEncoding encoding;

        // for sink
        private String mutationOp;
        private TimestampType timestampType;
        private String timestampField;
        private String timestampValue;

        // for source
        private CellType cellType;
        // schema
        private String type;
        private String mode;
        private List<ColumnQualifierProperties> fields;
        private List<String> symbols;
        private String valueType;

        private Schema.FieldType fieldType;

        private transient Template templateQualifier;
        private transient Template templateMutationOp;
        private transient Template templateType;
        private transient long fixedTimestampMicros;

        @Override
        public String toString() {
            return String.format("{ name: %s, field: %s, format: %s }", name, field, format);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMode() {
            return mode;
        }

        @Override
        public List<ColumnQualifierProperties> getFields() {
            return fields;
        }

        @Override
        public List<String> getSymbols() {
            return symbols;
        }

        @Override
        public String getValueType() {
            return valueType;
        }

        public static ColumnQualifierProperties of(final Schema.Field field) {
            final ColumnQualifierProperties qualifier = new ColumnQualifierProperties();
            qualifier.name = field.getName();
            qualifier.field = field.getName();
            return qualifier;
        }

        public List<String> validate(int i, int j) {
            final List<String> errorMessages = new ArrayList<>();
            if(name == null) {
                errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].name must not be null");
            }
            if(encoding != null) {
                errorMessages.addAll(encoding.validateConflict(format, "parameters.columns[" + i + "].qualifiers[" + j + "]"));
            }
            if(field == null && name == null) {
                errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].field must not be empty");
            }
            if(symbols == null) {
                symbols = new ArrayList<>();
            }
            if(timestampType != null) {
                switch (timestampType) {
                    case field -> {
                        if(timestampField == null) {
                            errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].timestampField must not be empty if timestampType is field");
                        }
                    }
                    case fixed -> {
                        if(timestampValue == null) {
                            errorMessages.add("parameters.columns[" + i + "].qualifiers[" + j + "].timestampValue must not be empty if timestampType is fixed");
                        }
                    }
                }
            }
            return errorMessages;
        }

        // for write
        public void setDefaults(
                final Format defaultFormat,
                final String defaultMutationOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue) {

            setDefaults(defaultFormat, defaultMutationOp,
                    defaultTimestampType, defaultTimestampField, defaultTimestampValue,
                    null);
        }

        // for read
        private void setDefaults(
                final Format defaultFormat,
                final String defaultMutationOp,
                final TimestampType defaultTimestampType,
                final String defaultTimestampField,
                final String defaultTimestampValue,
                final CellType defaultCellType) {

            if(field == null) {
                field = name;
            }
            if(format == null && encoding != null && encoding.format != null) {
                format = encoding.format;
            }
            if(format == null) {
                format = defaultFormat;
            }
            if(mutationOp == null) {
                mutationOp = defaultMutationOp;
            }
            if(timestampType == null) {
                timestampType = defaultTimestampType;
            }
            if(this.timestampField == null) {
                this.timestampField = defaultTimestampField;
            }
            if(this.timestampValue == null) {
                this.timestampValue = defaultTimestampValue;
            }
            if(cellType == null) {
                cellType = defaultCellType;
            }
        }

        public List<String> extractTemplateArgs(final Schema inputSchema) {
            final List<String> templateArgs = TemplateUtil.extractTemplateArgs(name, inputSchema);
            if(TemplateUtil.isTemplateText(mutationOp)) {
                templateArgs.addAll(TemplateUtil.extractTemplateArgs(mutationOp, inputSchema));
            }
            if(type != null && TemplateUtil.isTemplateText(type)) {
                templateArgs.addAll(TemplateUtil.extractTemplateArgs(type, inputSchema));
            }
            return templateArgs;
        }

        public void setupSource() {
            if(encoding != null && encoding.reference != null) {
                // encoding.reference supplies the Avro schema document for this cell
                // (schema-redesign.md P4); resolved once at assembly time
                final String json;
                if(encoding.reference.getInline() != null) {
                    json = encoding.reference.getInline();
                } else {
                    json = ResourceUtil.readString(encoding.reference.getUri());
                }
                final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(json);
                this.fieldType = Schema.FieldType.element(Schema.of(avroSchema));
            } else {
                this.fieldType = Schema.IField.toFieldType(this);
            }
            this.templateQualifier = TemplateUtil.createStrictTemplate("templateQualifier", name);
        }

        public void setupSink() {
            this.templateQualifier = TemplateUtil.createStrictTemplate("templateQualifier", name);
            if(TemplateUtil.isTemplateText(mutationOp)) {
                this.templateMutationOp = TemplateUtil.createStrictTemplate("templateMutationOp", mutationOp);
            }
            if(type != null && TemplateUtil.isTemplateText(type)) {
                this.templateType = TemplateUtil.createStrictTemplate("templateType", type);
            }
        }

        private Mutation toMutation(
                final String cf,
                final Map<String, Object> primitiveValues,
                final Map<String, Object> standardValues,
                final Instant timestamp) {

            final MutationOp resolvedMutationOp = resolveMutationOp(mutationOp, templateMutationOp, standardValues);
            final Object primitiveValue = primitiveValues.get(field);
            if(primitiveValue == null && !MutationOp.DELETE_FROM_COLUMN.equals(resolvedMutationOp)) {
                return null;
            }
            final Schema.Type dynamicType = getDynamicType(standardValues);

            final String cq = TemplateUtil.executeStrictTemplate(templateQualifier, standardValues);
            return switch (resolvedMutationOp) {
                case SET_CELL -> {
                    final ByteString fieldValue = toByteString(format, primitiveValue, dynamicType);
                    final long timestampMicros = switch (timestampType) {
                        case server -> -1L;
                        case event -> timestamp.getMillis() * 1000L;
                        case current -> DateTimeUtil.reduceAccuracy(DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()), 1000L);
                        case field -> DateTimeUtil.reduceAccuracy((Long) primitiveValues.get(timestampField), 1000L);
                        case fixed -> DateTimeUtil.toEpochMicroSecond(timestampValue);
                        case zero -> 0L;
                    };
                    final Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(ByteString.copyFrom(cq, StandardCharsets.UTF_8))
                            .setValue(fieldValue)
                            .setTimestampMicros(timestampMicros >= -1 ? timestampMicros : -1)
                            .build();
                    yield Mutation.newBuilder().setSetCell(cell).build();
                }
                case ADD_TO_CELL -> {
                    final long timestampMicros = switch (timestampType) {
                        case server -> -1L;
                        case event -> timestamp.getMillis() * 1000L;
                        case current -> DateTimeUtil.reduceAccuracy(DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()), 1000L);
                        case field -> DateTimeUtil.reduceAccuracy((Long) primitiveValues.get(timestampField), 1000L);
                        case fixed -> DateTimeUtil.toEpochMicroSecond(timestampValue);
                        case zero -> 0L;
                    };
                    final Mutation.AddToCell cell = Mutation.AddToCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(Value.newBuilder().setBytesValue(ByteString.copyFrom(cq, StandardCharsets.UTF_8)))
                            .setInput(toValue(primitiveValue))
                            .setTimestamp(Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(timestampMicros)))
                            .build();
                    yield Mutation.newBuilder().setAddToCell(cell).build();
                }
                case MERGE_TO_CELL -> {
                    final Mutation.MergeToCell cell = Mutation.MergeToCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(Value.newBuilder().setBytesValue(ByteString.copyFrom(cq, StandardCharsets.UTF_8)))
                            .setInput(toValue(primitiveValue))
                            .setTimestamp(Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(1L)))
                            .build();
                    yield Mutation.newBuilder().setMergeToCell(cell).build();
                }
                case DELETE_FROM_COLUMN -> Mutation.newBuilder()
                        .setDeleteFromColumn(Mutation.DeleteFromColumn.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(ByteString.copyFrom(cq, StandardCharsets.UTF_8))
                            .build())
                        .build();
                default -> throw new IllegalArgumentException("Illegal mutationOp: " + mutationOp + " for columnQualifier");
            };
        }

        private Schema.Type getDynamicType(final Map<String, Object> standardValues) {
            if(type == null) {
                return null;
            }
            final String resolvedType;
            if(templateType != null) {
                resolvedType = TemplateUtil.executeStrictTemplate(templateType, standardValues);
            } else {
                resolvedType = type;
            }
            if(resolvedType == null || resolvedType.isEmpty()) {
                return null;
            }
            return Schema.Type.of(resolvedType);
        }

        private List<Object> toPrimitiveValues(final Column column) {
            final List<Object> list = new ArrayList<>();
            for(final Cell c : column.getCellsList()) {
                final Object primitiveValue = BigtableSchemaUtil.toPrimitiveValue(format, fieldType, c.getValue());
                list.add(primitiveValue);
            }
            return list;
        }

        private Object toPrimitiveValue(final ByteString byteString) {
            return BigtableSchemaUtil.toPrimitiveValue(format, fieldType, byteString);
        }
    }

    public static Schema createSchema(final List<ColumnFamilyProperties> families) {
        final List<Schema.Field> fields = families.stream()
                .flatMap(f -> f.qualifiers.stream())
                .peek(ColumnQualifierProperties::setupSource)
                .map(q -> Schema.Field.of(q.field, q.fieldType))
                .toList();
        return Schema.builder().withFields(fields).build();
    }

    public static Schema createCellSchema() {
        return Schema.builder()
                .withField("rowKey", Schema.FieldType.STRING)
                .withField("family", Schema.FieldType.STRING)
                .withField("qualifier", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.BYTES)
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .build();
    }

    public static org.apache.avro.Schema createCellAvroSchema() {
        return SchemaBuilder.builder().record("root").fields()
                .name("rowKey").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("family").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("qualifier").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("value").type(AvroSchemaUtil.REQUIRED_BYTES).noDefault()
                .name("timestamp").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();
    }

    public static ModType getModType(final Entry entry) {
        return switch (entry) {
            case SetCell setCell -> ModType.SET_CELL;
            case DeleteFamily deleteFamily -> ModType.DELETE_FAMILY;
            case DeleteCells deleteCells -> ModType.DELETE_CELLS;
            default -> ModType.UNKNOWN;
        };
    }

    public static Schema createChangeRecordMutationSchemaA() {
        return Schema.builder()
                .withField("rowKey", Schema.FieldType.STRING)
                .withField("family", Schema.FieldType.STRING)
                .withField("qualifier", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.BYTES)
                .withField("timestamp", Schema.FieldType.TIMESTAMP)
                .build();
    }

    public static Schema createChangeRecordMutationSchema() {
        try (final InputStream is = FailureUtil.class.getResourceAsStream(RESOURCE_CDC_AVRO_SCHEMA_PATH)) {
            if(is == null) {
                //LOG.info("BadRecord avro file is not found: " + RESOURCE_CDC_AVRO_SCHEMA_PATH);
                try(final InputStream iss = Files.newInputStream(Path.of(RESOURCE_RUNTIME_CDC_AVRO_SCHEMA_PATH))) {
                    final String schemaJson = new String(iss.readAllBytes(), StandardCharsets.UTF_8);
                    final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(schemaJson);
                    return Schema.of(avroSchema);
                } catch (Throwable e) {
                    throw new IllegalArgumentException("BadRecord avro file is not found", e);
                }
            }
            final String schemaJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convertSchema(schemaJson);
            return Schema.of(avroSchema);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Not found event descriptor file", e);
        }
    }

    public static MElement convert(
            final ChangeStreamMutation mutation,
            final Instant timestamp) {

        ChangeStreamMutation.MutationType a = mutation.getType();

        final Map<String, Object> primitiveValues = new HashMap<>();
        primitiveValues.put("rowKey", mutation.getRowKey().asReadOnlyByteBuffer());
        primitiveValues.put("commitTimestamp", DateTimeUtil.toEpochMicroSecond(mutation.getCommitTime()));
        primitiveValues.put("tieBreaker", mutation.getTieBreaker());
        primitiveValues.put("sourceCluster", mutation.getSourceClusterId());
        primitiveValues.put("estimatedLowWatermarkTime", DateTimeUtil.toEpochMicroSecond(mutation.getEstimatedLowWatermarkTime()));

        final List<Map<String,Object>> entriesPrimitiveValues = new ArrayList<>();
        for(final Entry entry : mutation.getEntries()) {
            final Map<String, Object> entryPrimitiveValues = new HashMap<>();
            switch (entry) {
                case SetCell setCell -> {
                    entryPrimitiveValues.put("familyName", setCell.getFamilyName());
                    entryPrimitiveValues.put("qualifier", setCell.getQualifier().asReadOnlyByteBuffer());
                    entryPrimitiveValues.put("value", setCell.getValue().asReadOnlyByteBuffer());
                    entryPrimitiveValues.put("timestamp", setCell.getTimestamp());
                    entryPrimitiveValues.put("modType", ModType.SET_CELL.id);
                    entriesPrimitiveValues.add(entryPrimitiveValues);
                }
                case DeleteFamily deleteFamily -> {
                    entryPrimitiveValues.put("familyName", deleteFamily.getFamilyName());
                    entryPrimitiveValues.put("modType", ModType.DELETE_FAMILY.id);
                    entriesPrimitiveValues.add(entryPrimitiveValues);
                }
                case DeleteCells deleteCells -> {
                    entryPrimitiveValues.put("familyName", deleteCells.getFamilyName());
                    entryPrimitiveValues.put("qualifier", deleteCells.getQualifier().asReadOnlyByteBuffer());
                    entryPrimitiveValues.put("modType", ModType.DELETE_CELLS.id);
                    entriesPrimitiveValues.add(entryPrimitiveValues);
                }
                default -> {}
            }
        }

        primitiveValues.put("entries", entriesPrimitiveValues);

        return MElement.of(primitiveValues, timestamp);
    }

    public static Map<String, ColumnFamilyProperties> toMap(List<ColumnFamilyProperties> families) {
        final Map<String, ColumnFamilyProperties> map = new HashMap<>();
        if(families == null) {
            return map;
        }
        for(final ColumnFamilyProperties family : families) {
            map.put(family.family, family);
        }
        return map;
    }

    public static MutationOp resolveMutationOp(
            final String mutationOp,
            final Template templateOp,
            final Map<String, Object> templateVariables) {

        if(templateOp != null) {
            final String resolved = TemplateUtil.executeStrictTemplate(templateOp, templateVariables);
            return MutationOp.valueOf(resolved);
        }
        return MutationOp.valueOf(mutationOp);
    }

    public static List<Mutation> toMutations(
            final List<ColumnFamilyProperties> families,
            final Map<String, Object> primitiveValues,
            final Map<String, Object> standardValues,
            final Instant timestamp) {

        final List<Mutation> mutations = new ArrayList<>();
        for(var family : families) {
            final List<Mutation> m = family.toMutation(primitiveValues, standardValues, timestamp);
            mutations.addAll(m);
        }
        return mutations;
    }

    public static Map<String, Object> toPrimitiveValues(
            final Row row,
            final Map<String, ColumnFamilyProperties> familyProperties) {

        final Map<String, Object> primitiveValues = new HashMap<>();
        for(final Family family : row.getFamiliesList()) {
            if(!familyProperties.containsKey(family.getName())) {
                continue;
            }
            final ColumnFamilyProperties familyProperty = familyProperties.get(family.getName());
            final Map<String, Object> values = familyProperty.toElement(family);
            primitiveValues.putAll(values);
        }
        return primitiveValues;
    }

    public static Map<String, Object> toPrimitiveValues(
            final com.google.cloud.bigtable.data.v2.models.Row row,
            final Map<String, ColumnFamilyProperties> families) {

        final Map<String, Object> primitiveValues = new HashMap<>();
        for(final Map.Entry<String, ColumnFamilyProperties> entry : families.entrySet()) {
            for(final ColumnQualifierProperties qualifier : entry.getValue().qualifiers) {
                final List<RowCell> cells = row.getCells(entry.getKey(), qualifier.name);
                final Object primitiveValue = qualifier.toPrimitiveValue(cells.getFirst().getValue());
                primitiveValues.put(qualifier.field, primitiveValue);
            }
        }

        /*
        for(final RowCell cell : row.getCells()) {
            if(!families.containsKey(cell.getFamily())) {
                continue;
            }
            final ColumnFamilyProperties family = families.get(cell.getFamily());
            for(final ColumnQualifierProperties qualifier : family.qualifiers) {
                if(!cell.getQualifier().toStringUtf8().equals(qualifier.name)) {
                    continue;
                }
                final Object primitiveValue = qualifier.toPrimitiveValue(cell.getValue());
                primitiveValues.put(qualifier.field, primitiveValue);
            }
        }

         */
        return primitiveValues;
    }

    public static KV<Long, Long> getRowMinMaxTimestamps(final Row row) {
        long max = 0;
        long min = Long.MAX_VALUE;
        for(final Family family : row.getFamiliesList()) {
            for(final Column column : family.getColumnsList()) {
                for(final Cell cell : column.getCellsList()) {
                    if(cell.getTimestampMicros() > max) {
                        max = cell.getTimestampMicros();
                    }
                    if(cell.getTimestampMicros() < min) {
                        min = cell.getTimestampMicros();
                    }
                }
            }
        }
        return KV.of(min, max);
    }

    public static class ColumnSetting implements Serializable {

        private String field;
        private String columnFamily;
        private String columnQualifier;
        private Boolean exclude;
        private Format format;
        private MutationOp mutationOp;

        public String getField() {
            return field;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public String getColumnQualifier() {
            return columnQualifier;
        }

        public Boolean getExclude() {
            return exclude;
        }

        public Format getFormat() {
            return format;
        }

        public MutationOp getMutationOp() {
            return mutationOp;
        }

        public void setDefaults(final Format format, final String defaultColumnFamily, final MutationOp defaultMutationOp) {
            if (columnQualifier == null) {
                columnQualifier = field;
            }
            if (columnFamily == null) {
                columnFamily = defaultColumnFamily;
            }
            if (exclude == null) {
                exclude = false;
            }
            if (this.format == null) {
                this.format = format;
            }
            if (this.mutationOp == null) {
                this.mutationOp = defaultMutationOp;
            }
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (field == null) {
                errorMessages.add("BigtableSink module's mappings parameter requires `field` parameter.");
            }
            return errorMessages;
        }
    }

    private static ByteString toByteString(final Format format, final Object primitiveValue, final Schema.Type dynamicType) {
        return switch (format) {
            case text -> toByteStringText(primitiveValue);
            case bytes -> toByteStringBytes(primitiveValue, dynamicType);
            case avro -> {
                try {
                    final byte[] bytes = AvroSchemaUtil.encode(primitiveValue);
                    yield ByteString.copyFrom(bytes);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert to avro ByteString", e);
                }
            }
            case avromap -> {
                final Map<String, Object> values = new HashMap<>();
                switch (primitiveValue) {
                    case Map<?,?> map -> values.putAll((Map<String, Object>)map);
                    case List<?> list -> {
                        for(final Object listValue : list) {
                            if(listValue == null) {
                                continue;
                            }
                            if(!(listValue instanceof Map)) {
                                throw new IllegalArgumentException("avro");
                            }
                            final Map<?,?> map = (Map<?,?>) listValue;
                            if(!map.containsKey("key") || !map.containsKey("value")) {
                                throw new IllegalArgumentException("avromap format requires fields key and value. but input: " + map);
                            }
                            String key = (String) map.get("key");
                            Object value = map.get("value");
                            values.put(key, value);
                        }
                    }
                    default -> throw new IllegalArgumentException("avromap is not supported to convert byte string");
                }
                try {
                    final byte[] bytes = AvroSchemaUtil.encode(values);
                    yield ByteString.copyFrom(bytes);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert to avro ByteString", e);
                }
            }
            default -> throw new IllegalArgumentException("Not supported byte string convert format: " + format);
        };
    }

    // ByteBuffer.array() fails for read-only or direct buffers (e.g. ByteString.asReadOnlyByteBuffer())
    private static byte[] toBytes(final ByteBuffer byteBuffer) {
        final ByteBuffer duplicated = byteBuffer.duplicate();
        duplicated.rewind();
        final byte[] bytes = new byte[duplicated.remaining()];
        duplicated.get(bytes);
        return bytes;
    }

    public static ByteString toByteString(final Object primitiveValue) {
        if(primitiveValue == null) {
            return ByteString.copyFrom(new byte[0]);
        }
        final byte[] bytes = switch (primitiveValue) {
            case Boolean b -> HBaseBytes.toBytes(b);
            case String s -> HBaseBytes.toBytes(s);
            case byte[] bs -> bs;
            case ByteBuffer bb -> toBytes(bb);
            case ByteString bs -> bs.toByteArray();
            case ByteArray ba -> ba.toByteArray();
            case BigDecimal bd -> HBaseBytes.toBytes(bd);
            case Short s -> HBaseBytes.toBytes(s);
            case Integer i -> HBaseBytes.toBytes(i);
            case Long l -> HBaseBytes.toBytes(l);
            case Float f -> HBaseBytes.toBytes(f);
            case Double d -> HBaseBytes.toBytes(d);
            default -> throw new IllegalArgumentException("Not supported bytes class: " + primitiveValue.getClass());
        };
        return ByteString.copyFrom(bytes);
    }

    public static ByteString toByteStringText(final Object primitiveValue) {
        if(primitiveValue == null) {
            return ByteString.copyFrom(new byte[0]);
        }
        final String text = switch (primitiveValue) {
            case String s -> s;
            case Utf8 u -> u.toString();
            case byte[] bs -> Base64.getEncoder().encodeToString(bs);
            case ByteBuffer bb -> Base64.getEncoder().encodeToString(toBytes(bb));
            case ByteString bs -> Base64.getEncoder().encodeToString(bs.toByteArray());
            case ByteArray ba -> Base64.getEncoder().encodeToString(ba.toByteArray());
            default -> primitiveValue.toString();
        };
        return ByteString.copyFrom(text, StandardCharsets.UTF_8);
    }

    public static ByteString toByteStringBytes(Object primitiveValue, final Schema.Type dynamicType) {
        if(primitiveValue == null) {
            return null;
        }
        primitiveValue = convertDynamicFieldValue(dynamicType, primitiveValue);
        final byte[] bytes = switch (primitiveValue) {
            case Boolean b -> HBaseBytes.toBytes(b);
            case String s -> HBaseBytes.toBytes(s);
            case Utf8 u -> HBaseBytes.toBytes(u.toString());
            case byte[] bs -> bs;
            case ByteBuffer bb -> toBytes(bb);
            case ByteString bs -> bs.toByteArray();
            case ByteArray ba -> ba.toByteArray();
            case BigDecimal bd -> HBaseBytes.toBytes(bd);
            case Short s -> HBaseBytes.toBytes(s);
            case Integer i -> HBaseBytes.toBytes(i);
            case Long l -> HBaseBytes.toBytes(l);
            case Float f -> HBaseBytes.toBytes(f);
            case Double d -> HBaseBytes.toBytes(d);
            default -> throw new IllegalArgumentException("Not supported bytes class: " + primitiveValue.getClass());
        };
        return ByteString.copyFrom(bytes);
    }

    public static Object convertDynamicFieldValue(final Schema.Type dynamicType, final Object primitiveValue) {
        if(dynamicType == null) {
            return primitiveValue;
        }
        if(primitiveValue == null) {
            return null;
        }
        return switch (dynamicType) {
            case string, json -> primitiveValue.toString();
            case bool -> switch (primitiveValue) {
                case String s -> Boolean.parseBoolean(s);
                case Utf8 u -> Boolean.parseBoolean(u.toString());
                case Number n -> n.doubleValue() > 0;
                default -> null;
            };
            case int16 -> switch (primitiveValue) {
                case String s -> Short.parseShort(s);
                case Utf8 u -> Short.parseShort(u.toString());
                case Boolean b -> b ? (short) 1 : (short) 0;
                case Number n -> n.shortValue();
                default -> null;
            };
            case int32 -> switch (primitiveValue) {
                case String s -> Integer.parseInt(s);
                case Utf8 u -> Integer.parseInt(u.toString());
                case Boolean b -> b ? 1 : 0;
                case Number n -> n.intValue();
                default -> null;
            };
            case int64 -> switch (primitiveValue) {
                case String s -> Long.parseLong(s);
                case Utf8 u -> Long.parseLong(u.toString());
                case Boolean b -> b ? 1L : 0L;
                case Number n -> n.longValue();
                default -> null;
            };
            case float32 -> switch (primitiveValue) {
                case String s -> Float.parseFloat(s);
                case Utf8 u -> Float.parseFloat(u.toString());
                case Boolean b -> b ? 1F : 0F;
                case Number n -> n.floatValue();
                default -> null;
            };
            case float64 -> switch (primitiveValue) {
                case String s -> Double.parseDouble(s);
                case Utf8 u -> Double.parseDouble(u.toString());
                case Boolean b -> b ? 1D : 0D;
                case Number n -> n.doubleValue();
                default -> null;
            };
            case date -> switch (primitiveValue) {
                case String s -> DateTimeUtil.toEpochDay(s);
                case Utf8 u -> DateTimeUtil.toEpochDay(u.toString());
                case Number n -> n.intValue();
                default -> null;
            };
            case time -> switch (primitiveValue) {
                case String s -> DateTimeUtil.toMicroOfDay(s);
                case Utf8 u -> DateTimeUtil.toMicroOfDay(u.toString());
                case Number n -> n.longValue();
                default -> null;
            };
            case timestamp -> switch (primitiveValue) {
                case String s -> DateTimeUtil.toEpochMicroSecond(s);
                case Utf8 u -> DateTimeUtil.toEpochMicroSecond(u.toString());
                case Number n -> n.longValue();
                default -> null;
            };
            default -> throw new RuntimeException("Bigtable dynamic field type does not support type: " + dynamicType);
        };
    }

    public static Object toPrimitiveValue(final Format format, final Schema.FieldType fieldtype, final ByteString byteString) {
        return switch (format) {
            case bytes -> toPrimitiveValueFromBytes(fieldtype, byteString);
            case text -> ElementSchemaUtil.getAsPrimitive(fieldtype, new String(byteString.toByteArray(), StandardCharsets.UTF_8));
            case avro, avromap -> {
                try {
                    yield AvroSchemaUtil.decode(fieldtype, byteString.toByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static Object toPrimitiveValueFromBytes(final Schema.FieldType fieldtype, final ByteString byteString) {
        if (byteString == null) {
            return null;
        }
        final byte[] bytes = byteString.toByteArray();
        return switch (fieldtype.getType()) {
            case bool -> HBaseBytes.toBoolean(bytes);
            case string, json -> HBaseBytes.toString(bytes);
            case bytes -> ByteBuffer.wrap(bytes);
            case int16 -> HBaseBytes.toShort(bytes);
            case int32, date, enumeration -> HBaseBytes.toInt(bytes);
            case int64, time, timestamp -> HBaseBytes.toLong(bytes);
            case float32 -> HBaseBytes.toFloat(bytes);
            case float64 -> HBaseBytes.toDouble(bytes);
            default -> throw new IllegalArgumentException("Not supported deserialize type: " + fieldtype.getType());
        };
    }

    private static Value toValue(final Object primitiveValue) {
        return switch (primitiveValue) {
            case Boolean b -> Value.newBuilder().setBoolValue(b).build();
            case String s -> Value.newBuilder().setStringValue(s).build();
            case Integer i -> Value.newBuilder().setIntValue(i).build();
            case Long l -> Value.newBuilder().setIntValue(l).build();
            case Float f -> Value.newBuilder().setFloatValue(f).build();
            case Double d -> Value.newBuilder().setFloatValue(d).build();
            case ByteBuffer bb -> Value.newBuilder().setBytesValue(ByteString.copyFrom(bb)).build();
            case byte[] b -> Value.newBuilder().setBytesValue(ByteString.copyFrom(b)).build();
            default -> throw new IllegalArgumentException();
        };
    }

}
