package com.mercari.solution.util.coder;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes Spanner Struct values without serializing non-semantic SDK state.
 *
 * <p>Spanner's {@link Type#getFieldIndex(String)} lazily initializes the {@code fieldsByName}
 * lookup cache held by a STRUCT {@link Type}. Name-based Struct getters call that method. Although
 * the SDK documents initialization of this immutable cache as a benign race, {@code fieldsByName}
 * is not transient and is therefore included by Java serialization. Consequently,
 * {@link SerializableCoder} produces different bytes before and after a name-based getter, causing
 * DirectRunner's immutability check to report that the input element was mutated even though its
 * schema and values did not change.
 *
 * <p>This coder creates a canonical copy using index-based reads before delegating to Java
 * serialization. The copy preserves the Struct's logical type and values while leaving the SDK's
 * field-name cache uninitialized, so coder output is independent of whether callers previously
 * used name-based getters.
 */
public class StructCoder extends AtomicCoder<Struct> {

    private static final StructCoder INSTANCE = new StructCoder();
    private static final SerializableCoder<Struct> DELEGATE = SerializableCoder.of(Struct.class);

    public static StructCoder of() {
        return INSTANCE;
    }

    private StructCoder() {}

    @Override
    public void encode(final Struct value, final OutputStream outStream) throws IOException {
        encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(final Struct value, final OutputStream outStream, final Context context) throws IOException {
        DELEGATE.encode(canonicalize(value), outStream, context);
    }

    @Override
    public Struct decode(final InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public Struct decode(final InputStream inStream, final Context context) throws IOException {
        return DELEGATE.decode(inStream, context);
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {
        DELEGATE.verifyDeterministic();
    }

    private static Struct canonicalize(final Struct struct) {
        if(struct == null) {
            return null;
        }
        final Struct.Builder builder = Struct.newBuilder();
        final List<Type.StructField> fields = struct.getType().getStructFields();
        for(int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            final Type.StructField field = fields.get(fieldIndex);
            builder.set(field.getName()).to(canonicalize(struct.getValue(fieldIndex)));
        }
        return builder.build();
    }

    private static Value canonicalize(final Value value) {
        if(value == null) {
            return null;
        }
        return switch (value.getType().getCode()) {
            case STRUCT -> Value.struct(canonicalize(value.getType()),
                    value.isNull() ? null : canonicalize(value.getStruct()));
            case ARRAY -> {
                final Type elementType = value.getType().getArrayElementType();
                if(!Type.Code.STRUCT.equals(elementType.getCode())) {
                    yield value;
                }
                if(value.isNull()) {
                    yield Value.structArray(canonicalize(elementType), null);
                }
                final List<Struct> structs = new ArrayList<>();
                for(final Struct struct : value.getStructArray()) {
                    structs.add(canonicalize(struct));
                }
                yield Value.structArray(canonicalize(elementType), structs);
            }
            default -> value;
        };
    }

    private static Type canonicalize(final Type type) {
        return switch (type.getCode()) {
            case BOOL -> Type.bool();
            case INT64 -> Type.int64();
            case NUMERIC -> Type.numeric();
            case PG_NUMERIC -> Type.pgNumeric();
            case FLOAT64 -> Type.float64();
            case FLOAT32 -> Type.float32();
            case STRING -> Type.string();
            case JSON -> Type.json();
            case PG_JSONB -> Type.pgJsonb();
            case PG_OID -> Type.pgOid();
            case PROTO -> Type.proto(type.getProtoTypeFqn());
            case ENUM -> Type.protoEnum(type.getProtoTypeFqn());
            case BYTES -> Type.bytes();
            case TIMESTAMP -> Type.timestamp();
            case DATE -> Type.date();
            case UUID -> Type.uuid();
            case INTERVAL -> Type.interval();
            case ARRAY -> Type.array(canonicalize(type.getArrayElementType()));
            case STRUCT -> {
                final List<Type.StructField> fields = new ArrayList<>();
                for(final Type.StructField field : type.getStructFields()) {
                    fields.add(Type.StructField.of(field.getName(), canonicalize(field.getType())));
                }
                yield Type.struct(fields);
            }
            case UNRECOGNIZED -> type;
        };
    }
}
