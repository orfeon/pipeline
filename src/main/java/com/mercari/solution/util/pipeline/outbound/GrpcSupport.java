package com.mercari.solution.util.pipeline.outbound;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
import io.grpc.protobuf.ProtoUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Descriptor-set driven gRPC plumbing shared by the grpc lookup source and the grpc sink
 * (the {@code grpcurl} mechanism: no generated stubs, requests/responses travel as
 * {@link DynamicMessage}s): linking a protoc descriptor set, resolving a method, building
 * its {@link MethodDescriptor}, and a channel interceptor that adds static headers plus an
 * {@link AuthProvider}'s headers as call metadata.
 */
public final class GrpcSupport {

    private GrpcSupport() {}

    /** Links a protoc {@code --descriptor_set_out --include_imports} file into FileDescriptors keyed by file name. */
    public static Map<String, Descriptors.FileDescriptor> linkDescriptorSet(final byte[] bytes) {
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

    private static Descriptors.FileDescriptor linkFile(final String name,
            final Map<String, FileDescriptorProto> protos,
            final Map<String, Descriptors.FileDescriptor> built) {
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

    /** Resolves {@code pkg.Service/Method} (or {@code pkg.Service.Method}) in the linked files. */
    public static Descriptors.MethodDescriptor resolveMethod(
            final Map<String, Descriptors.FileDescriptor> files, final String fullMethodName) {
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
        for (final Descriptors.FileDescriptor fd : files.values()) {
            for (final Descriptors.ServiceDescriptor service : fd.getServices()) {
                if (service.getFullName().equals(serviceName)) {
                    final Descriptors.MethodDescriptor method = service.findMethodByName(methodName);
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

    /** A dynamic-message MethodDescriptor for the proto method with the given call type. */
    public static MethodDescriptor<DynamicMessage, DynamicMessage> methodDescriptor(
            final Descriptors.MethodDescriptor method, final MethodDescriptor.MethodType type) {
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(type)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(
                        method.getService().getFullName(), method.getName()))
                .setRequestMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(method.getInputType())))
                .setResponseMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(method.getOutputType())))
                .build();
    }

    /** The call type declared by the proto method. */
    public static MethodDescriptor.MethodType methodType(final Descriptors.MethodDescriptor method) {
        if (method.isClientStreaming() && method.isServerStreaming()) {
            return MethodDescriptor.MethodType.BIDI_STREAMING;
        }
        if (method.isClientStreaming()) {
            return MethodDescriptor.MethodType.CLIENT_STREAMING;
        }
        if (method.isServerStreaming()) {
            return MethodDescriptor.MethodType.SERVER_STREAMING;
        }
        return MethodDescriptor.MethodType.UNARY;
    }

    public static ManagedChannel createChannel(final String target, final boolean plaintext, final int maxInboundMessageBytes) {
        final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
        if (plaintext) {
            builder.usePlaintext();
        }
        if (maxInboundMessageBytes > 0) {
            builder.maxInboundMessageSize(maxInboundMessageBytes);
        }
        return builder.build();
    }

    /** Wraps the channel so every call carries the static headers and the auth provider's headers (as metadata). */
    public static Channel withHeaders(final ManagedChannel channel, final Map<String, String> headers, final AuthProvider auth) {
        if ((headers == null || headers.isEmpty()) && (auth == null || auth.isNone())) {
            return channel;
        }
        final Metadata extra = new Metadata();
        if (headers != null) {
            for (final Map.Entry<String, String> e : headers.entrySet()) {
                extra.put(Metadata.Key.of(e.getKey().toLowerCase(Locale.ROOT), Metadata.ASCII_STRING_MARSHALLER), e.getValue());
            }
        }
        final ClientInterceptor interceptor = new ClientInterceptor() {
            @Override
            public <Q, S> ClientCall<Q, S> interceptCall(MethodDescriptor<Q, S> method,
                    CallOptions callOptions, Channel next) {
                return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<S> responseListener, Metadata requestHeaders) {
                        requestHeaders.merge(extra);
                        if (auth != null && !auth.isNone()) {
                            try {
                                for (final Map.Entry<String, String> e : auth.headers().entrySet()) {
                                    requestHeaders.put(Metadata.Key.of(e.getKey().toLowerCase(Locale.ROOT),
                                            Metadata.ASCII_STRING_MARSHALLER), e.getValue());
                                }
                            } catch (IOException e) {
                                throw new IllegalStateException("failed to obtain auth credentials for gRPC call", e);
                            }
                        }
                        super.start(responseListener, requestHeaders);
                    }
                };
            }
        };
        return ClientInterceptors.intercept(channel, interceptor);
    }
}
