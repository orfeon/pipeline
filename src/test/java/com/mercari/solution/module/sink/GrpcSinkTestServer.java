package com.mercari.solution.module.sink;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.*;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Descriptor-driven in-JVM gRPC server for the grpc sink tests (no protoc, no stubs).
 * <pre>
 * message Item        { string id = 1; string name = 2; double price = 3; }
 * message ItemAck     { string id = 1; int64 version = 2; }
 * message BulkRequest { repeated Item items = 1; string tenant = 2; }
 * message BulkAck     { int64 count = 1; bool ok = 2; }
 * service ItemService {
 *   rpc Upsert(Item) returns (ItemAck);                  // unary; id "flaky" fails UNAVAILABLE once; id "bad" INVALID_ARGUMENT
 *   rpc BulkUpsert(BulkRequest) returns (BulkAck);       // unary with a repeated field
 *   rpc StreamUpsert(stream Item) returns (BulkAck);     // client streaming
 * }
 * </pre>
 */
final class GrpcSinkTestServer {

    final FileDescriptor file;
    final Descriptor item;
    final Descriptor itemAck;
    final Descriptor bulkRequest;
    final Descriptor bulkAck;

    final List<String> upserted = Collections.synchronizedList(new ArrayList<>());
    final List<List<String>> bulks = Collections.synchronizedList(new ArrayList<>());
    final List<String> metadataSeen = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger flakyCalls = new AtomicInteger();
    final AtomicReference<String> requiredToken = new AtomicReference<>();

    private final Server server;

    GrpcSinkTestServer() throws Exception {
        this.file = buildFile();
        this.item = file.findMessageTypeByName("Item");
        this.itemAck = file.findMessageTypeByName("ItemAck");
        this.bulkRequest = file.findMessageTypeByName("BulkRequest");
        this.bulkAck = file.findMessageTypeByName("BulkAck");
        final ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                metadataSeen.add(headers.get(Metadata.Key.of("x-tenant", Metadata.ASCII_STRING_MARSHALLER))
                        + "|" + headers.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)));
                final String required = requiredToken.get();
                if (required != null) {
                    final String auth = headers.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER));
                    if (auth == null || !auth.equals("Bearer " + required)) {
                        call.close(Status.UNAUTHENTICATED.withDescription("bad token"), new Metadata());
                        return new ServerCall.Listener<>() {};
                    }
                }
                return next.startCall(call, headers);
            }
        };
        this.server = ServerBuilder.forPort(0)
                .intercept(interceptor)
                .addService(itemService())
                .build()
                .start();
    }

    int port() {
        return server.getPort();
    }

    Path writeDescriptorSet(Path dir) throws IOException {
        final Path path = dir.resolve("items.desc");
        Files.write(path, FileDescriptorSet.newBuilder().addFile(file.toProto()).build().toByteArray());
        return path;
    }

    void shutdown() {
        server.shutdownNow();
    }

    private ServerServiceDefinition itemService() {
        return ServerServiceDefinition.builder("demo.ItemService")
                .addMethod(grpcMethod("Upsert", item, itemAck, MethodDescriptor.MethodType.UNARY),
                        ServerCalls.asyncUnaryCall((req, obs) -> {
                            final String id = (String) req.getField(item.findFieldByName("id"));
                            if ("bad".equals(id)) {
                                obs.onError(Status.INVALID_ARGUMENT.withDescription("bad id").asRuntimeException());
                                return;
                            }
                            if ("flaky".equals(id) && flakyCalls.incrementAndGet() == 1) {
                                obs.onError(Status.UNAVAILABLE.withDescription("try again").asRuntimeException());
                                return;
                            }
                            upserted.add(id + ":" + req.getField(item.findFieldByName("name")) + ":" + req.getField(item.findFieldByName("price")));
                            obs.onNext(DynamicMessage.newBuilder(itemAck)
                                    .setField(itemAck.findFieldByName("id"), id)
                                    .setField(itemAck.findFieldByName("version"), (long) upserted.size())
                                    .build());
                            obs.onCompleted();
                        }))
                .addMethod(grpcMethod("BulkUpsert", bulkRequest, bulkAck, MethodDescriptor.MethodType.UNARY),
                        ServerCalls.asyncUnaryCall((req, obs) -> {
                            final List<String> ids = new ArrayList<>();
                            for (final Object o : (List<?>) req.getField(bulkRequest.findFieldByName("items"))) {
                                ids.add((String) ((DynamicMessage) o).getField(item.findFieldByName("id")));
                            }
                            bulks.add(ids);
                            obs.onNext(DynamicMessage.newBuilder(bulkAck)
                                    .setField(bulkAck.findFieldByName("count"), (long) ids.size())
                                    .setField(bulkAck.findFieldByName("ok"), !ids.contains("reject"))
                                    .build());
                            obs.onCompleted();
                        }))
                .addMethod(grpcMethod("StreamUpsert", item, bulkAck, MethodDescriptor.MethodType.CLIENT_STREAMING),
                        ServerCalls.asyncClientStreamingCall(responseObserver -> new StreamObserver<DynamicMessage>() {
                            private final List<String> ids = new ArrayList<>();
                            @Override
                            public void onNext(DynamicMessage value) {
                                ids.add((String) value.getField(item.findFieldByName("id")));
                            }
                            @Override
                            public void onError(Throwable t) {}
                            @Override
                            public void onCompleted() {
                                bulks.add(ids);
                                responseObserver.onNext(DynamicMessage.newBuilder(bulkAck)
                                        .setField(bulkAck.findFieldByName("count"), (long) ids.size())
                                        .setField(bulkAck.findFieldByName("ok"), true)
                                        .build());
                                responseObserver.onCompleted();
                            }
                        }))
                .build();
    }

    private static MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod(
            String method, Descriptor in, Descriptor out, MethodDescriptor.MethodType type) {
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(type)
                .setFullMethodName(MethodDescriptor.generateFullMethodName("demo.ItemService", method))
                .setRequestMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(in)))
                .setResponseMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(out)))
                .build();
    }

    private static FileDescriptor buildFile() throws DescriptorValidationException {
        final FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("items.proto")
                .setSyntax("proto3")
                .setPackage("demo")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Item")
                        .addField(field("id", 1, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null))
                        .addField(field("name", 2, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null))
                        .addField(field("price", 3, Type.TYPE_DOUBLE, Label.LABEL_OPTIONAL, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("ItemAck")
                        .addField(field("id", 1, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null))
                        .addField(field("version", 2, Type.TYPE_INT64, Label.LABEL_OPTIONAL, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("BulkRequest")
                        .addField(field("items", 1, Type.TYPE_MESSAGE, Label.LABEL_REPEATED, ".demo.Item"))
                        .addField(field("tenant", 2, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("BulkAck")
                        .addField(field("count", 1, Type.TYPE_INT64, Label.LABEL_OPTIONAL, null))
                        .addField(field("ok", 2, Type.TYPE_BOOL, Label.LABEL_OPTIONAL, null)))
                .addService(ServiceDescriptorProto.newBuilder()
                        .setName("ItemService")
                        .addMethod(MethodDescriptorProto.newBuilder().setName("Upsert").setInputType(".demo.Item").setOutputType(".demo.ItemAck"))
                        .addMethod(MethodDescriptorProto.newBuilder().setName("BulkUpsert").setInputType(".demo.BulkRequest").setOutputType(".demo.BulkAck"))
                        .addMethod(MethodDescriptorProto.newBuilder().setName("StreamUpsert").setInputType(".demo.Item").setOutputType(".demo.BulkAck").setClientStreaming(true)))
                .build();
        return FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
    }

    private static FieldDescriptorProto field(String name, int number, Type type, Label label, String typeName) {
        final FieldDescriptorProto.Builder b = FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type).setLabel(label);
        if (typeName != null) {
            b.setTypeName(typeName);
        }
        return b.build();
    }
}
