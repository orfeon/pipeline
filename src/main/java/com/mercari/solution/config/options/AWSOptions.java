package com.mercari.solution.config.options;

import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.Serializable;
import java.net.URI;

/**
 * Maps the config {@code options.aws} block onto Beam's {@link AwsOptions}, which both the
 * s3 filesystem and aws2 IOs consume.
 *
 * See docs/developer/cloud-auth.md §5.1. Only credentials provider types that Beam's
 * AwsModule can serialize to workers may be emitted here.
 */
public class AWSOptions implements Serializable {

    private String region;
    private String endpoint;
    private CredentialsOptions credentials;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final AWSOptions aws) {

        if(aws == null) {
            return;
        }

        final AwsOptions awsOptions = pipelineOptions.as(AwsOptions.class);
        if(aws.region != null) {
            awsOptions.setAwsRegion(Region.of(aws.region));
        }
        if(aws.endpoint != null) {
            awsOptions.setEndpoint(URI.create(aws.endpoint));
        }
        if(aws.credentials != null) {
            awsOptions.setAwsCredentialsProvider(aws.credentials.createProvider(aws));
        }
    }

    public static class CredentialsOptions implements Serializable {

        private String type;
        // static
        private String accessKey;
        private String secretKey;
        private String sessionToken;
        // assumeRole
        private String roleArn;
        private String roleSessionName;
        private String externalId;
        private Integer durationSeconds;

        private String resolveType() {
            if(type != null) {
                return type;
            }
            if(roleArn != null) {
                return "assumeRole";
            }
            if(accessKey != null || secretKey != null) {
                return "static";
            }
            return "default";
        }

        private AwsCredentialsProvider createProvider(final AWSOptions aws) {
            return switch (resolveType()) {
                case "default" -> DefaultCredentialsProvider.create();
                case "static" -> createStaticProvider();
                case "assumeRole" -> createAssumeRoleProvider(aws);
                default -> throw new IllegalArgumentException(
                        "options.aws.credentials.type must be one of [default, static, assumeRole] but was: " + type);
            };
        }

        private AwsCredentialsProvider createStaticProvider() {
            if(accessKey == null || secretKey == null) {
                throw new IllegalArgumentException(
                        "options.aws.credentials with type 'static' requires both accessKey and secretKey");
            }
            if(sessionToken != null) {
                return StaticCredentialsProvider.create(
                        AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
            }
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey));
        }

        private AwsCredentialsProvider createAssumeRoleProvider(final AWSOptions aws) {
            if(roleArn == null) {
                throw new IllegalArgumentException(
                        "options.aws.credentials with type 'assumeRole' requires roleArn");
            }
            final AssumeRoleRequest.Builder request = AssumeRoleRequest.builder()
                    .roleArn(roleArn)
                    .roleSessionName(roleSessionName != null ? roleSessionName : "mercari-pipeline");
            if(externalId != null) {
                request.externalId(externalId);
            }
            if(durationSeconds != null) {
                request.durationSeconds(durationSeconds);
            }
            final StsClientBuilder sts = StsClient.builder();
            if(aws.region != null) {
                sts.region(Region.of(aws.region));
            }
            if(aws.endpoint != null) {
                sts.endpointOverride(URI.create(aws.endpoint));
            }
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(sts.build())
                    .refreshRequest(request.build())
                    .build();
        }

    }

}
