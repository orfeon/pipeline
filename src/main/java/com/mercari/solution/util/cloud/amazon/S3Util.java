package com.mercari.solution.util.cloud.amazon;

import org.apache.beam.sdk.io.aws2.options.S3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.InstanceBuilder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.Map;


public class S3Util {

    /**
     * Builds the client through the same factory chain as Beam's s3 filesystem
     * ({@code S3Options.s3ClientFactoryClass}), so construction-time access and runtime IO
     * share one credential source — including {@code gcpFederation}
     * (docs/design/cloud-auth.md §5.3).
     */
    public static S3Client storage(final PipelineOptions pipelineOptions) {
        final S3Options s3Options = pipelineOptions.as(S3Options.class);
        return InstanceBuilder
                .ofType(S3ClientBuilderFactory.class)
                .fromClass(s3Options.getS3ClientFactoryClass())
                .build()
                .createBuilder(s3Options)
                .build();
    }

    public static byte[] readBytes(final S3Client s3, final String s3Path) {
        final String[] paths = parseS3Path(s3Path);
        return readBytes(s3, paths[0], paths[1]);
    }

    public static void writeBytes(
            final S3Client s3,
            final String s3Path,
            final byte[] content,
            final String type,
            final Map<String, Object> attributes,
            final Map<String, String> metadata) {

        final String[] paths = parseS3Path(s3Path);

        PutObjectRequest.Builder builder = PutObjectRequest.builder().bucket(paths[0]).key(paths[1]).contentType(type).metadata(metadata);
        for(Map.Entry<String, Object> entry : attributes.entrySet()) {
            switch (entry.getKey()) {
                case "storageClass":
                    builder = builder.storageClass((String)entry.getValue());
                case "objectLockMode":
                    builder = builder.objectLockMode((String)entry.getValue());
                case "bucketKeyEnabled":
                    builder = builder.bucketKeyEnabled((Boolean)entry.getValue());
                case "redirectLocation":
                    builder = builder.websiteRedirectLocation((String)entry.getValue());
            }
        }
        final RequestBody body = RequestBody.fromBytes(content);
        s3.putObject(builder.build(), body);
    }

    public static void copy(final S3Client s3, final String sourcePath, final String destinationPath, final Map<String, Object> attributes) {
        final String[] sourcePaths = parseS3Path(sourcePath);
        final String[] destinationPaths = parseS3Path(destinationPath);
        CopyObjectRequest.Builder builder = CopyObjectRequest.builder()
                .sourceBucket(sourcePaths[0])
                .sourceKey(sourcePaths[1])
                .destinationBucket(destinationPaths[0])
                .destinationKey(destinationPaths[1]);
        for(Map.Entry<String, Object> entry : attributes.entrySet()) {
            switch (entry.getKey()) {
                case "storageClass":
                    builder = builder.storageClass((String)entry.getValue());
                case "objectLockMode":
                    builder = builder.objectLockMode((String)entry.getValue());
                case "bucketKeyEnabled":
                    builder = builder.bucketKeyEnabled((Boolean)entry.getValue());
                case "redirectLocation":
                    builder = builder.websiteRedirectLocation((String)entry.getValue());
            }
        }
        s3.copyObject(builder.build());
    }

    private static String[] parseS3Path(String s3Path) {
        if(s3Path == null) {
            throw new IllegalArgumentException("gcsPath must not be null");
        }
        if(!s3Path.startsWith("s3://")) {
            throw new IllegalArgumentException("s3Path must start with s3://");
        }
        final String[] paths = s3Path.replaceAll("s3://", "").split("/", 2);
        if(paths.length != 2) {
            throw new IllegalArgumentException("Illegal gcsPath: " + s3Path);
        }
        return paths;
    }

    private static byte[] readBytes(final S3Client s3, final String bucket, final String object) {
        try {
            return s3.getObject(GetObjectRequest.builder().bucket(bucket).key(object).build()).readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
