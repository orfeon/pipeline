package com.mercari.solution.util.pipeline.lookup.source;

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
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * A self-contained, descriptor-driven gRPC server for tests (ported from
 * orfeon/calcite-multi-engine). Both the proto contract and the server are
 * built dynamically — no protoc, no generated stubs: the {@link FileDescriptor}
 * is assembled from {@link FileDescriptorProto}s, the matching {@code .desc}
 * bytes are written for the source to load, and the handlers serve
 * {@link DynamicMessage}s — exactly the runtime contract {@link GrpcLookupSource}
 * relies on. Binds a real localhost socket (ephemeral port), since the source
 * connects via a {@code target} string.
 *
 * <p>Contract (package {@code demo}):
 * <pre>
 * message GetUserRequest  { int64 id = 1; }
 * message Address         { string city = 1; string zip = 2; }
 * message User            { int64 id = 1; string name = 2; Address address = 3; repeated string tags = 4; }
 * message ClassifyRequest { string text = 1; }
 * message Label           { string name = 1; double score = 2; }
 * message ClassifyResponse{ repeated Label labels = 1; }
 * service UserService { rpc GetUser(GetUserRequest) returns (User); }                 // unary, 1 row
 * service Classifier  { rpc Classify(ClassifyRequest) returns (ClassifyResponse); }   // unary, rowsFrom=labels
 * service Streamer    { rpc Stream(ClassifyRequest) returns (stream Label); }         // server streaming
 * </pre>
 */
final class DynamicGrpcTestServer {

    final FileDescriptor file;
    final Descriptor getUserRequest;
    final Descriptor user;
    final Descriptor address;
    final Descriptor classifyRequest;
    final Descriptor classifyResponse;
    final Descriptor label;

    private final Server server;

    DynamicGrpcTestServer() throws Exception {
        this.file = buildFile();
        this.getUserRequest = file.findMessageTypeByName("GetUserRequest");
        this.user = file.findMessageTypeByName("User");
        this.address = file.findMessageTypeByName("Address");
        this.classifyRequest = file.findMessageTypeByName("ClassifyRequest");
        this.classifyResponse = file.findMessageTypeByName("ClassifyResponse");
        this.label = file.findMessageTypeByName("Label");
        this.server = ServerBuilder.forPort(0)
                .addService(userService())
                .addService(classifierService())
                .addService(streamerService())
                .build()
                .start();
    }

    /** The bound ephemeral port. */
    int port() {
        return server.getPort();
    }

    /** Writes the {@code .desc} file the source loads via descriptorSetPath. */
    Path writeDescriptorSet(Path dir) throws IOException {
        final Path path = dir.resolve("demo.desc");
        Files.write(path, FileDescriptorSet.newBuilder()
                .addFile(file.toProto()).build().toByteArray());
        return path;
    }

    void shutdown() {
        server.shutdownNow();
    }

    // ---- handlers ----------------------------------------------------------

    private ServerServiceDefinition userService() {
        final MethodDescriptor<DynamicMessage, DynamicMessage> method = grpcMethod(
                "demo.UserService", "GetUser", getUserRequest, user,
                MethodDescriptor.MethodType.UNARY);
        return ServerServiceDefinition.builder("demo.UserService")
                .addMethod(method, ServerCalls.asyncUnaryCall((req, obs) -> {
                    final long id = (Long) req.getField(getUserRequest.findFieldByName("id"));
                    obs.onNext(buildUser(id));
                    obs.onCompleted();
                }))
                .build();
    }

    private ServerServiceDefinition classifierService() {
        final MethodDescriptor<DynamicMessage, DynamicMessage> method = grpcMethod(
                "demo.Classifier", "Classify", classifyRequest, classifyResponse,
                MethodDescriptor.MethodType.UNARY);
        return ServerServiceDefinition.builder("demo.Classifier")
                .addMethod(method, ServerCalls.asyncUnaryCall((req, obs) -> {
                    final String text =
                            (String) req.getField(classifyRequest.findFieldByName("text"));
                    final DynamicMessage.Builder resp = DynamicMessage.newBuilder(classifyResponse);
                    resp.setField(classifyResponse.findFieldByName("labels"), labelsFor(text));
                    obs.onNext(resp.build());
                    obs.onCompleted();
                }))
                .build();
    }

    private ServerServiceDefinition streamerService() {
        final MethodDescriptor<DynamicMessage, DynamicMessage> method = grpcMethod(
                "demo.Streamer", "Stream", classifyRequest, label,
                MethodDescriptor.MethodType.SERVER_STREAMING);
        return ServerServiceDefinition.builder("demo.Streamer")
                .addMethod(method, ServerCalls.asyncServerStreamingCall((req, obs) -> {
                    final String text =
                            (String) req.getField(classifyRequest.findFieldByName("text"));
                    for (final Object l : labelsFor(text)) {
                        obs.onNext((DynamicMessage) l);
                    }
                    obs.onCompleted();
                }))
                .build();
    }

    // ---- data --------------------------------------------------------------

    private static final Map<Long, String[]> USERS = Map.of(
            1L, new String[] {"alice", "NYC", "10001"},
            2L, new String[] {"bob", "LA", "90001"});

    private DynamicMessage buildUser(long id) {
        final String[] u = USERS.getOrDefault(id, new String[] {"unknown", "?", "?"});
        final DynamicMessage addr = DynamicMessage.newBuilder(address)
                .setField(address.findFieldByName("city"), u[1])
                .setField(address.findFieldByName("zip"), u[2])
                .build();
        final DynamicMessage.Builder b = DynamicMessage.newBuilder(user)
                .setField(user.findFieldByName("id"), id)
                .setField(user.findFieldByName("name"), u[0])
                .setField(user.findFieldByName("address"), addr);
        b.setField(user.findFieldByName("tags"),
                id == 1L ? List.of("a", "b") : List.of());
        return b.build();
    }

    private List<DynamicMessage> labelsFor(String text) {
        final FieldDescriptor name = label.findFieldByName("name");
        final FieldDescriptor score = label.findFieldByName("score");
        return switch (text) {
            case "spanner" -> List.of(
                    DynamicMessage.newBuilder(label)
                            .setField(name, "db").setField(score, 0.9).build(),
                    DynamicMessage.newBuilder(label)
                            .setField(name, "cloud").setField(score, 0.5).build());
            case "bigtable" -> List.of(
                    DynamicMessage.newBuilder(label)
                            .setField(name, "nosql").setField(score, 0.8).build());
            default -> List.of();
        };
    }

    // ---- descriptor + grpc plumbing ---------------------------------------

    private static MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod(
            String service, String method, Descriptor in, Descriptor out,
            MethodDescriptor.MethodType type) {
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(type)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(service, method))
                .setRequestMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(in)))
                .setResponseMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(out)))
                .build();
    }

    private static FileDescriptor buildFile() throws DescriptorValidationException {
        final FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("demo.proto")
                .setSyntax("proto3")
                .setPackage("demo")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("GetUserRequest")
                        .addField(field("id", 1, Type.TYPE_INT64, Label.LABEL_OPTIONAL, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Address")
                        .addField(field("city", 1, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null))
                        .addField(field("zip", 2, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("User")
                        .addField(field("id", 1, Type.TYPE_INT64, Label.LABEL_OPTIONAL, null))
                        .addField(field("name", 2, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null))
                        .addField(field("address", 3, Type.TYPE_MESSAGE, Label.LABEL_OPTIONAL,
                                ".demo.Address"))
                        .addField(field("tags", 4, Type.TYPE_STRING, Label.LABEL_REPEATED, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("ClassifyRequest")
                        .addField(field("text", 1, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Label")
                        .addField(field("name", 1, Type.TYPE_STRING, Label.LABEL_OPTIONAL, null))
                        .addField(field("score", 2, Type.TYPE_DOUBLE, Label.LABEL_OPTIONAL, null)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("ClassifyResponse")
                        .addField(field("labels", 1, Type.TYPE_MESSAGE, Label.LABEL_REPEATED,
                                ".demo.Label")))
                .addService(ServiceDescriptorProto.newBuilder()
                        .setName("UserService")
                        .addMethod(MethodDescriptorProto.newBuilder()
                                .setName("GetUser")
                                .setInputType(".demo.GetUserRequest")
                                .setOutputType(".demo.User")))
                .addService(ServiceDescriptorProto.newBuilder()
                        .setName("Classifier")
                        .addMethod(MethodDescriptorProto.newBuilder()
                                .setName("Classify")
                                .setInputType(".demo.ClassifyRequest")
                                .setOutputType(".demo.ClassifyResponse")))
                .addService(ServiceDescriptorProto.newBuilder()
                        .setName("Streamer")
                        .addMethod(MethodDescriptorProto.newBuilder()
                                .setName("Stream")
                                .setInputType(".demo.ClassifyRequest")
                                .setOutputType(".demo.Label")
                                .setServerStreaming(true)))
                .build();
        return FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
    }

    private static FieldDescriptorProto.Builder field(String name, int number, Type type,
            Label label, String typeName) {
        final FieldDescriptorProto.Builder b = FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type).setLabel(label);
        if (typeName != null) {
            b.setTypeName(typeName);
        }
        return b;
    }
}
