package com.mercari.solution.util.pipeline.lookup.source;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.pipeline.lookup.PerKeyLookup;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * gRPC-backed {@link LookupSource}: exposes RPC methods of <em>any</em> gRPC
 * service as key-driven lookup tables. No service-specific stubs are compiled
 * in — the contract is supplied as a protoc <b>descriptor set</b>
 * ({@code protoc --include_imports --descriptor_set_out=service.desc ...}), and
 * requests/responses travel as {@link DynamicMessage}s (the {@code grpcurl}
 * mechanism), so one build works against any service. The grpc/protobuf runtime
 * is already on the classpath via the Beam GCP IO — this source adds no
 * dependency.
 *
 * <p>Each table is one RPC method keyed by a single <em>query-key</em> column —
 * the request parameter carried by the input row ({@code JOIN grpc.users AS u
 * ON u.id = i.userId}). One gRPC call is issued per distinct key (a unary RPC
 * takes a single request; point equality only). Result granularity: a unary
 * method without {@code rowsFrom} maps the whole response to one row (repeated
 * response fields become array columns); {@code rowsFrom} names a top-level
 * repeated message field to fan out over (one row per element); a
 * server-streaming method yields one row per streamed message.
 *
 * <p>The row schema is <b>derived from the descriptor set</b> when {@code fields}
 * is omitted: int32→int32, int64→int64, float/double→float64 (float32 is always
 * widened — the engine's value convention), bool, string, bytes, enum→string
 * (by name), {@code google.protobuf.Timestamp}→timestamp, singular message→
 * nested element row, repeated→array. The query-key column is prepended when
 * the response does not echo it (typed from the bound request field). Explicit
 * {@code fields} override the derived response columns (e.g. to expose a
 * subset). Derivation happens once at pipeline construction (the launcher
 * reads the descriptor set file); workers reuse the serialized schema and
 * descriptor bytes.
 *
 * <p><b>Read-only / idempotent by contract</b>: {@code lookup()} runs many
 * times, in arbitrary order, and bundle retries repeat it — only expose RPCs
 * that are safe to call repeatedly.
 */
public class GrpcLookupSource extends LookupSource {

    private static final String TIMESTAMP_FULL_NAME = "google.protobuf.Timestamp";

    /** One exposed table = one RPC method keyed by a query-key column. */
    public static class TableConfig implements Serializable {

        private final String name;
        private final String method;           // package.Service/Method
        private final String keyField;         // query-key column name
        private final String requestKeyField;  // dotted request field path
        private final boolean serverStreaming;
        private final String rowsFrom;
        private final String requestTemplate;  // constant request fields (protobuf JSON)
        private final List<Schema.Field> declaredFields;

        // Derived at setup on the launcher; serialized so workers skip derivation.
        private Schema schema;
        private int keyFieldIndex = -1;

        private TableConfig(TableBuilder builder) {
            this.name = builder.name;
            this.method = builder.method;
            this.keyField = builder.keyField;
            this.requestKeyField = builder.requestKeyField != null
                    ? builder.requestKeyField : builder.keyField;
            this.serverStreaming = builder.serverStreaming;
            this.rowsFrom = builder.rowsFrom;
            this.requestTemplate = builder.requestTemplate;
            this.declaredFields = List.copyOf(builder.fields);
            if (name == null || method == null || keyField == null) {
                throw new IllegalArgumentException(
                        "grpc table requires name, method and keyField");
            }
            if (rowsFrom != null && serverStreaming) {
                throw new IllegalArgumentException("grpc table '" + name + "': rowsFrom and"
                        + " serverStreaming are mutually exclusive (a streaming method already"
                        + " yields one row per message)");
            }
        }

        public static TableBuilder builder() {
            return new TableBuilder();
        }
    }

    public static class TableBuilder {

        private String name;
        private String method;
        private String keyField;
        private String requestKeyField;
        private boolean serverStreaming;
        private String rowsFrom;
        private String requestTemplate;
        private final List<Schema.Field> fields = new ArrayList<>();

        public TableBuilder withName(String name) {
            this.name = name;
            return this;
        }

        /** Fully-qualified RPC method, {@code package.Service/Method}. */
        public TableBuilder withMethod(String method) {
            this.method = method;
            return this;
        }

        /** Column carrying the request parameter — the join key. */
        public TableBuilder withKeyField(String keyField) {
            this.keyField = keyField;
            return this;
        }

        /**
         * Request field the key is bound to (dotted path for a nested field,
         * e.g. {@code filter.userId}); defaults to the keyField name.
         */
        public TableBuilder withRequestKeyField(String requestKeyField) {
            this.requestKeyField = requestKeyField;
            return this;
        }

        /** Mark the method server-streaming (one row per streamed message). */
        public TableBuilder withServerStreaming(boolean serverStreaming) {
            this.serverStreaming = serverStreaming;
            return this;
        }

        /** Top-level repeated message response field to fan out over (unary only). */
        public TableBuilder withRowsFrom(String rowsFrom) {
            this.rowsFrom = rowsFrom;
            return this;
        }

        /** Constant request fields as protobuf JSON (the key is overlaid on top). */
        public TableBuilder withRequestTemplate(String requestTemplate) {
            this.requestTemplate = requestTemplate;
            return this;
        }

        /** Explicit response columns, overriding descriptor-set derivation. */
        public TableBuilder withFields(List<Schema.Field> fields) {
            this.fields.addAll(fields);
            return this;
        }

        public TableConfig build() {
            return new TableConfig(this);
        }
    }

    private final String target;
    private final boolean plaintext;
    private final Map<String, String> headers;
    private final long deadlineMillis;
    private final int maxInboundMessageBytes;
    private final String descriptorSetPath;
    private final Map<String, TableConfig> tables;

    // Read from descriptorSetPath at first setup (on the launcher) and shipped
    // to the workers, so only the launcher needs the file.
    private byte[] descriptorSetBytes;

    private transient Map<String, Descriptors.FileDescriptor> descriptorFiles;
    private transient ManagedChannel managedChannel;
    private transient Channel channel;
    private transient Map<String, MethodDescriptor<DynamicMessage, DynamicMessage>> grpcMethods;

    private GrpcLookupSource(Builder builder) {
        super(builder.name);
        this.target = builder.target;
        this.plaintext = builder.plaintext;
        this.headers = Map.copyOf(builder.headers);
        this.deadlineMillis = builder.deadlineMillis;
        this.maxInboundMessageBytes = builder.maxInboundMessageBytes;
        this.descriptorSetPath = builder.descriptorSetPath;
        this.descriptorSetBytes = builder.descriptorSetBytes;
        if (target == null) {
            throw new IllegalArgumentException("grpc lookup source requires target");
        }
        if (descriptorSetPath == null && descriptorSetBytes == null) {
            throw new IllegalArgumentException(
                    "grpc lookup source requires descriptorSetPath (or descriptorSetBytes)");
        }
        final Map<String, TableConfig> map = new LinkedHashMap<>();
        for (final TableConfig table : builder.tables) {
            map.put(table.name, table);
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException("grpc lookup source requires at least one table");
        }
        this.tables = map;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private String target;
        private boolean plaintext;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private long deadlineMillis = 60_000L;
        private int maxInboundMessageBytes;
        private String descriptorSetPath;
        private byte[] descriptorSetBytes;
        private final List<TableConfig> tables = new ArrayList<>();

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        /** gRPC target: {@code host:port} or any name-resolver URI. */
        public Builder withTarget(String target) {
            this.target = target;
            return this;
        }

        /** Use plaintext instead of TLS (local/in-cluster targets). */
        public Builder withPlaintext(boolean plaintext) {
            this.plaintext = plaintext;
            return this;
        }

        /** A static request header sent on every call (e.g. an auth token). */
        public Builder withHeader(String name, String value) {
            this.headers.put(name, value);
            return this;
        }

        /** Per-call deadline in milliseconds (0 = none). */
        public Builder withDeadlineMillis(long deadlineMillis) {
            this.deadlineMillis = deadlineMillis;
            return this;
        }

        public Builder withMaxInboundMessageBytes(int maxInboundMessageBytes) {
            this.maxInboundMessageBytes = maxInboundMessageBytes;
            return this;
        }

        /** Path to the protoc descriptor set file (readable on the launcher). */
        public Builder withDescriptorSetPath(String descriptorSetPath) {
            this.descriptorSetPath = descriptorSetPath;
            return this;
        }

        /** The descriptor set bytes directly (alternative to the path). */
        public Builder withDescriptorSetBytes(byte[] descriptorSetBytes) {
            this.descriptorSetBytes = descriptorSetBytes;
            return this;
        }

        public Builder withTable(TableConfig table) {
            this.tables.add(table);
            return this;
        }

        public GrpcLookupSource build() {
            return new GrpcLookupSource(this);
        }
    }

    // ---- lifecycle ---------------------------------------------------------

    @Override
    protected void setupInternal() {
        if (descriptorSetBytes == null) {
            try {
                descriptorSetBytes = Files.readAllBytes(Path.of(descriptorSetPath));
            } catch (IOException e) {
                throw new IllegalStateException(
                        "failed to read gRPC descriptor set file: " + descriptorSetPath, e);
            }
        }
        if (descriptorFiles == null) {
            descriptorFiles = linkDescriptorSet(descriptorSetBytes);
        }
        for (final TableConfig table : tables.values()) {
            if (table.schema == null) {
                deriveSchema(table);
            }
        }
        if (managedChannel == null) {
            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
            if (plaintext) {
                channelBuilder.usePlaintext();
            }
            if (maxInboundMessageBytes > 0) {
                channelBuilder.maxInboundMessageSize(maxInboundMessageBytes);
            }
            managedChannel = channelBuilder.build();
            channel = withHeaders(managedChannel, headers);
            grpcMethods = new ConcurrentHashMap<>();
        }
    }

    @Override
    protected void closeInternal() {
        if (managedChannel != null) {
            managedChannel.shutdown();
            try {
                if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                    managedChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                managedChannel.shutdownNow();
            }
            managedChannel = null;
            channel = null;
            grpcMethods = null;
        }
    }

    @Override
    public Map<String, Schema> tableSchemas() {
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        for (final TableConfig table : tables.values()) {
            schemas.put(table.name, table.schema);
        }
        return schemas;
    }

    @Override
    public List<LookupKey> keyCandidates(String table) {
        final TableConfig config = tables.get(table);
        return config == null ? List.of() : List.of(LookupKey.primaryKey(List.of(config.keyField)));
    }

    // ---- lookup ------------------------------------------------------------

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        final TableConfig config = tables.get(table);
        if (config == null) {
            throw new IllegalStateException("unknown lookup table: " + getName() + "." + table);
        }
        final Descriptors.MethodDescriptor method = resolveMethod(config.method);
        final Descriptors.Descriptor requestType = method.getInputType();
        final Descriptors.FieldDescriptor rowsFromField =
                resolveRowsFrom(config, method.getOutputType());
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(config.schema.countFields());
        return PerKeyLookup.run(batch, "gRPC table '" + getName() + "." + table + "'",
                keyValues -> {
                    final DynamicMessage request =
                            buildRequest(config, requestType, keyValues.get(0));
                    return rowSources(call(config, request), rowsFromField);
                },
                (message, keyValues) -> decodeRow(config, message, outCols, keyValues));
    }

    /** Calls the table's method once; unary → one message, streaming → the sequence. */
    private List<DynamicMessage> call(TableConfig config, DynamicMessage request) {
        final MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod =
                grpcMethods.computeIfAbsent(config.name, n -> buildGrpcMethod(config));
        final CallOptions options = deadlineMillis > 0
                ? CallOptions.DEFAULT.withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS)
                : CallOptions.DEFAULT;
        try {
            if (config.serverStreaming) {
                final Iterator<DynamicMessage> it = ClientCalls.blockingServerStreamingCall(
                        channel, grpcMethod, options, request);
                final List<DynamicMessage> out = new ArrayList<>();
                while (it.hasNext()) {
                    out.add(it.next());
                }
                return out;
            }
            return List.of(ClientCalls.blockingUnaryCall(channel, grpcMethod, options, request));
        } catch (StatusRuntimeException e) {
            throw new IllegalStateException(
                    "gRPC call '" + config.method + "' failed: " + e.getStatus(), e);
        }
    }

    private MethodDescriptor<DynamicMessage, DynamicMessage> buildGrpcMethod(TableConfig config) {
        final Descriptors.MethodDescriptor method = resolveMethod(config.method);
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(config.serverStreaming
                        ? MethodDescriptor.MethodType.SERVER_STREAMING
                        : MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(
                        method.getService().getFullName(), method.getName()))
                .setRequestMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(method.getInputType())))
                .setResponseMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(method.getOutputType())))
                .build();
    }

    /** Expands the raw response(s) into the messages that each become one row. */
    private static List<Message> rowSources(List<DynamicMessage> responses,
            Descriptors.FieldDescriptor rowsFromField) {
        if (rowsFromField == null) {
            return new ArrayList<>(responses);
        }
        final Message response = responses.getFirst();
        final int count = response.getRepeatedFieldCount(rowsFromField);
        final List<Message> sources = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            sources.add((Message) response.getRepeatedField(rowsFromField, i));
        }
        return sources;
    }

    private Descriptors.FieldDescriptor resolveRowsFrom(TableConfig config,
            Descriptors.Descriptor responseType) {
        if (config.rowsFrom == null) {
            return null;
        }
        final Descriptors.FieldDescriptor fd = responseType.findFieldByName(config.rowsFrom);
        if (fd == null || !fd.isRepeated()
                || fd.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            throw new IllegalStateException("rowsFrom='" + config.rowsFrom + "' of grpc table '"
                    + config.name + "' must name a repeated message field of response type '"
                    + responseType.getFullName() + "'");
        }
        return fd;
    }

    // ---- request building ---------------------------------------------------

    /** Builds the request message: JSON template (if any) with the key overlaid. */
    private static DynamicMessage buildRequest(TableConfig config,
            Descriptors.Descriptor requestType, Object internalKeyValue) {
        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(requestType);
        if (config.requestTemplate != null && !config.requestTemplate.isBlank()) {
            try {
                JsonFormat.parser().ignoringUnknownFields()
                        .merge(config.requestTemplate, builder);
            } catch (IOException e) {
                throw new IllegalStateException("invalid requestTemplate JSON for request type '"
                        + requestType.getFullName() + "': " + e.getMessage(), e);
            }
        }
        final String[] parts = config.requestKeyField.split("\\.");
        Message.Builder current = builder;
        Descriptors.Descriptor currentType = requestType;
        for (int i = 0; i < parts.length - 1; i++) {
            final Descriptors.FieldDescriptor field = currentType.findFieldByName(parts[i]);
            if (field == null
                    || field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE
                    || field.isRepeated()) {
                throw new IllegalStateException("request key path segment '" + parts[i]
                        + "' is not a singular message field of '" + currentType.getFullName()
                        + "'");
            }
            current = current.getFieldBuilder(field);
            currentType = field.getMessageType();
        }
        final Descriptors.FieldDescriptor leaf =
                currentType.findFieldByName(parts[parts.length - 1]);
        if (leaf == null) {
            throw new IllegalStateException("request type '" + currentType.getFullName()
                    + "' has no field '" + parts[parts.length - 1] + "' for the query key");
        }
        current.setField(leaf, encodeScalar(leaf, internalKeyValue));
        return builder.build();
    }

    /** Calcite-internal key value → protobuf request field value. */
    private static Object encodeScalar(Descriptors.FieldDescriptor field, Object value) {
        return switch (field.getJavaType()) {
            case INT -> ((Number) value).intValue();
            case LONG -> ((Number) value).longValue();
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BOOLEAN -> (Boolean) value;
            case STRING -> value.toString();
            case BYTE_STRING -> ByteString.copyFrom(
                    ((org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString) value)
                            .getBytes());
            case ENUM -> {
                final Descriptors.EnumValueDescriptor ev =
                        field.getEnumType().findValueByName(value.toString());
                if (ev == null) {
                    throw new IllegalStateException("enum '" + field.getEnumType().getFullName()
                            + "' has no value named '" + value + "'");
                }
                yield ev;
            }
            case MESSAGE -> throw new IllegalStateException("query key field '" + field.getName()
                    + "' must be a scalar request field, not a message");
        };
    }

    // ---- response decoding ---------------------------------------------------

    /**
     * Decodes one result-source message into a row in {@code outCols} order. The
     * query-key column is filled from the request key (the response need not echo
     * it); other columns are read from the message by field name.
     */
    private static Object[] decodeRow(TableConfig config, Message message, int[] outCols,
            List<Object> keyValues) {
        final Object[] row = new Object[outCols.length];
        for (int i = 0; i < outCols.length; i++) {
            final int col = outCols[i];
            if (col == config.keyFieldIndex) {
                row[i] = keyValues.getFirst();
            } else {
                final Schema.Field field = config.schema.getField(col);
                row[i] = decodeField(message, field.getName(), field.getFieldType());
            }
        }
        return row;
    }

    private static Object decodeField(Message message, String fieldName,
            Schema.FieldType fieldType) {
        final Descriptors.FieldDescriptor fd =
                message.getDescriptorForType().findFieldByName(fieldName);
        if (fd == null) {
            throw new IllegalStateException("response message '"
                    + message.getDescriptorForType().getFullName()
                    + "' has no field '" + fieldName + "' (matching the schema field)");
        }
        if (fd.isRepeated()) {
            final Schema.FieldType element = fieldType.getArrayValueType() != null
                    ? fieldType.getArrayValueType() : fieldType;
            final int count = message.getRepeatedFieldCount(fd);
            final List<Object> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                list.add(decodeValue(message.getRepeatedField(fd, i), element));
            }
            return list;
        }
        if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                && !message.hasField(fd)) {
            return null;
        }
        return decodeValue(message.getField(fd), fieldType);
    }

    /** Protobuf field value → Calcite-internal value by schema field type. */
    private static Object decodeValue(Object raw, Schema.FieldType fieldType) {
        if (raw == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case string -> raw instanceof Descriptors.EnumValueDescriptor ev
                    ? ev.getName() : raw.toString();
            case json -> raw instanceof Message m ? printJson(m) : raw.toString();
            case int8, int16, int32 -> ((Number) raw).intValue();
            case int64 -> ((Number) raw).longValue();
            // float32 is widened: the engine maps float32 to Calcite FLOAT
            // (8-byte), so a Float value would CCE in generated code.
            case float32, float64 -> ((Number) raw).doubleValue();
            case bool -> raw;
            case decimal -> new BigDecimal(raw.toString());
            case bytes ->
                    new org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString(
                            ((ByteString) raw).toByteArray());
            case date -> ((Number) raw).intValue();
            case timestamp -> decodeTimestamp(raw);
            case element -> decodeStruct((Message) raw, fieldType.getElementSchema());
            default -> throw new IllegalStateException(
                    "unsupported grpc result type: " + fieldType.getType());
        };
    }

    /** Nested message → Object[] in the nested schema's field order. */
    private static Object[] decodeStruct(Message message, Schema schema) {
        final Object[] values = new Object[schema.countFields()];
        for (int i = 0; i < schema.countFields(); i++) {
            final Schema.Field field = schema.getField(i);
            values[i] = decodeField(message, field.getName(), field.getFieldType());
        }
        return values;
    }

    /** google.protobuf.Timestamp (or a numeric epoch-millis field) → epoch millis. */
    private static long decodeTimestamp(Object raw) {
        if (raw instanceof Message msg
                && TIMESTAMP_FULL_NAME.equals(msg.getDescriptorForType().getFullName())) {
            final Descriptors.Descriptor d = msg.getDescriptorForType();
            final long seconds = (Long) msg.getField(d.findFieldByName("seconds"));
            final int nanos = (Integer) msg.getField(d.findFieldByName("nanos"));
            return seconds * 1000L + nanos / 1_000_000L;
        }
        return ((Number) raw).longValue();
    }

    private static String printJson(Message message) {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(message);
        } catch (IOException e) {
            throw new IllegalStateException("failed to print protobuf message as JSON: "
                    + message.getDescriptorForType().getFullName(), e);
        }
    }

    // ---- schema derivation ----------------------------------------------------

    /**
     * Derives the table's row schema from the descriptor set: the query-key
     * column (prepended when the row-element message does not echo it, typed
     * from the bound request field) plus the element message's fields — or the
     * explicitly declared fields instead of the latter.
     */
    private void deriveSchema(TableConfig config) {
        final Descriptors.MethodDescriptor method = resolveMethod(config.method);
        final Descriptors.Descriptor element = elementMessage(config, method);
        final List<Schema.Field> fields = new ArrayList<>();

        final Map<String, Schema.FieldType> declared = new LinkedHashMap<>();
        for (final Schema.Field field : config.declaredFields) {
            declared.put(field.getName(), field.getFieldType());
        }
        if (element.findFieldByName(config.keyField) == null || declared.containsKey(config.keyField)) {
            final Schema.FieldType keyType = declared.containsKey(config.keyField)
                    ? declared.get(config.keyField)
                    : requestKeyType(config, method.getInputType());
            fields.add(Schema.Field.of(config.keyField, keyType));
        }
        if (declared.isEmpty()) {
            final TypeMapper mapper = new TypeMapper(config);
            for (final Descriptors.FieldDescriptor fd : element.getFields()) {
                fields.add(Schema.Field.of(fd.getName(), mapper.fieldType(fd)));
            }
        } else {
            for (final Schema.Field field : config.declaredFields) {
                if (!field.getName().equals(config.keyField)) {
                    fields.add(field);
                }
            }
        }
        config.schema = Schema.of(fields);
        config.keyFieldIndex = -1;
        for (int i = 0; i < config.schema.countFields(); i++) {
            if (config.schema.getField(i).getName().equals(config.keyField)) {
                config.keyFieldIndex = i;
                break;
            }
        }
        if (config.keyFieldIndex < 0) {
            throw new IllegalStateException("keyField '" + config.keyField
                    + "' is not present in the schema of grpc table '" + config.name + "'");
        }
    }

    /** The message whose fields become one output row (fan-out element or whole response). */
    private Descriptors.Descriptor elementMessage(TableConfig config,
            Descriptors.MethodDescriptor method) {
        if (config.serverStreaming) {
            return method.getOutputType();
        }
        if (config.rowsFrom != null) {
            return resolveRowsFrom(config, method.getOutputType()).getMessageType();
        }
        return method.getOutputType();
    }

    /** Resolves the bound request field (dotted path) and returns its schema type. */
    private Schema.FieldType requestKeyType(TableConfig config,
            Descriptors.Descriptor requestType) {
        Descriptors.Descriptor current = requestType;
        final String[] parts = config.requestKeyField.split("\\.");
        for (int i = 0; i < parts.length - 1; i++) {
            final Descriptors.FieldDescriptor f = current.findFieldByName(parts[i]);
            if (f == null || f.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE
                    || f.isRepeated()) {
                throw new IllegalStateException("request key path segment '" + parts[i]
                        + "' is not a singular message field of '" + current.getFullName()
                        + "' (grpc table '" + config.name + "')");
            }
            current = f.getMessageType();
        }
        final Descriptors.FieldDescriptor leaf = current.findFieldByName(parts[parts.length - 1]);
        if (leaf == null) {
            throw new IllegalStateException("request type '" + current.getFullName()
                    + "' has no field '" + parts[parts.length - 1] + "' for the query key of"
                    + " grpc table '" + config.name + "'");
        }
        return new TypeMapper(config).scalarOrMessage(leaf);
    }

    /** protobuf → schema type mapping with recursion guard for nested messages. */
    private final class TypeMapper {

        private final TableConfig config;
        private final Set<String> inProgress = new HashSet<>();
        private final Map<String, Schema> nested = new HashMap<>();

        private TypeMapper(TableConfig config) {
            this.config = config;
        }

        Schema.FieldType fieldType(Descriptors.FieldDescriptor fd) {
            if (fd.isRepeated()) {
                return Schema.FieldType.array(scalarOrMessage(fd));
            }
            return scalarOrMessage(fd);
        }

        Schema.FieldType scalarOrMessage(Descriptors.FieldDescriptor fd) {
            return switch (fd.getJavaType()) {
                case INT -> Schema.FieldType.INT32;
                case LONG -> Schema.FieldType.INT64;
                // float32 is deliberately surfaced as float64 (the engine's
                // convention: Calcite FLOAT is 8-byte; Float values CCE).
                case FLOAT, DOUBLE -> Schema.FieldType.FLOAT64;
                case BOOLEAN -> Schema.FieldType.BOOLEAN;
                case STRING, ENUM -> Schema.FieldType.STRING;
                case BYTE_STRING -> Schema.FieldType.BYTES;
                case MESSAGE -> messageType(fd.getMessageType());
            };
        }

        private Schema.FieldType messageType(Descriptors.Descriptor message) {
            if (TIMESTAMP_FULL_NAME.equals(message.getFullName())) {
                return Schema.FieldType.TIMESTAMP;
            }
            final String full = message.getFullName();
            final Schema cached = nested.get(full);
            if (cached != null) {
                return Schema.FieldType.element(cached);
            }
            if (!inProgress.add(full)) {
                throw new IllegalStateException("recursive protobuf message '" + full
                        + "' cannot be mapped to a schema; declare fields explicitly for"
                        + " grpc table '" + config.name + "'");
            }
            final List<Schema.Field> fields = new ArrayList<>();
            for (final Descriptors.FieldDescriptor fd : message.getFields()) {
                fields.add(Schema.Field.of(fd.getName(), fieldType(fd)));
            }
            inProgress.remove(full);
            final Schema schema = Schema.of(fields);
            nested.put(full, schema);
            return Schema.FieldType.element(schema);
        }
    }

    // ---- descriptor set --------------------------------------------------------

    /** Parses and links a protoc descriptor set (with its import graph). */
    private static Map<String, Descriptors.FileDescriptor> linkDescriptorSet(byte[] bytes) {
        final FileDescriptorSet set;
        try {
            set = FileDescriptorSet.parseFrom(bytes);
        } catch (IOException e) {
            throw new IllegalStateException("not a valid protoc FileDescriptorSet (use"
                    + " --descriptor_set_out --include_imports)", e);
        }
        final Map<String, FileDescriptorProto> protos = new LinkedHashMap<>();
        for (final FileDescriptorProto proto : set.getFileList()) {
            protos.put(proto.getName(), proto);
        }
        final Map<String, Descriptors.FileDescriptor> built = new LinkedHashMap<>();
        for (final FileDescriptorProto proto : set.getFileList()) {
            linkFile(proto.getName(), protos, built);
        }
        return built;
    }

    private static Descriptors.FileDescriptor linkFile(String name,
            Map<String, FileDescriptorProto> protos,
            Map<String, Descriptors.FileDescriptor> built) {
        final Descriptors.FileDescriptor existing = built.get(name);
        if (existing != null) {
            return existing;
        }
        final FileDescriptorProto proto = protos.get(name);
        if (proto == null) {
            throw new IllegalStateException("descriptor set is missing the imported proto file '"
                    + name + "'; regenerate it with protoc --include_imports");
        }
        final Descriptors.FileDescriptor[] deps =
                new Descriptors.FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < deps.length; i++) {
            deps[i] = linkFile(proto.getDependency(i), protos, built);
        }
        final Descriptors.FileDescriptor fd;
        try {
            fd = Descriptors.FileDescriptor.buildFrom(proto, deps);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "failed to link proto file '" + name + "': " + e.getMessage(), e);
        }
        built.put(name, fd);
        return fd;
    }

    /**
     * Resolves a method by its fully-qualified name in either
     * {@code pkg.Service/Method} (gRPC form) or {@code pkg.Service.Method} form.
     */
    private Descriptors.MethodDescriptor resolveMethod(String fullMethodName) {
        final String serviceName;
        final String methodName;
        final int slash = fullMethodName.indexOf('/');
        if (slash >= 0) {
            serviceName = fullMethodName.substring(0, slash);
            methodName = fullMethodName.substring(slash + 1);
        } else {
            final int dot = fullMethodName.lastIndexOf('.');
            if (dot < 0) {
                throw new IllegalStateException(
                        "grpc method must be 'package.Service/Method': " + fullMethodName);
            }
            serviceName = fullMethodName.substring(0, dot);
            methodName = fullMethodName.substring(dot + 1);
        }
        for (final Descriptors.FileDescriptor fd : descriptorFiles.values()) {
            for (final Descriptors.ServiceDescriptor service : fd.getServices()) {
                if (service.getFullName().equals(serviceName)) {
                    final Descriptors.MethodDescriptor method =
                            service.findMethodByName(methodName);
                    if (method != null) {
                        return method;
                    }
                }
            }
        }
        throw new IllegalStateException("method '" + fullMethodName + "' not found in the gRPC"
                + " descriptor set (expected service '" + serviceName + "', method '"
                + methodName + "')");
    }

    /** Wraps the channel so every call carries the configured static headers. */
    private static Channel withHeaders(ManagedChannel channel, Map<String, String> headers) {
        if (headers.isEmpty()) {
            return channel;
        }
        final Metadata extra = new Metadata();
        for (final Map.Entry<String, String> e : headers.entrySet()) {
            extra.put(Metadata.Key.of(e.getKey(), Metadata.ASCII_STRING_MARSHALLER), e.getValue());
        }
        final ClientInterceptor interceptor = new ClientInterceptor() {
            @Override
            public <Q, S> ClientCall<Q, S> interceptCall(MethodDescriptor<Q, S> method,
                    CallOptions callOptions, Channel next) {
                return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<S> responseListener, Metadata requestHeaders) {
                        requestHeaders.merge(extra);
                        super.start(responseListener, requestHeaders);
                    }
                };
            }
        };
        return ClientInterceptors.intercept(channel, interceptor);
    }
}
