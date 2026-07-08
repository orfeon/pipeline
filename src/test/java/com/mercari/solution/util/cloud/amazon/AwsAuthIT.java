package com.mercari.solution.util.cloud.amazon;

import com.mercari.solution.config.Options;
import com.mercari.solution.util.cloud.SecretProviders;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretResponse;

/**
 * Integration test (run via maven-failsafe:
 * {@code mvn verify -DskipITs=false -Djib.skip=true -Dit.test=AwsAuthIT})
 * for the cross-cloud auth wiring (docs/developer/cloud-auth.md) against LocalStack:
 *
 * <ul>
 *   <li>{@code options.aws} (region/endpoint/credentials) flowing through Beam's s3
 *       filesystem — s3:// read/write/exists via {@code ResourceUtil} and the
 *       {@code S3ClientBuilderFactory} chain used by {@code S3Util.storage}.</li>
 *   <li>{@code SecretProviders} resolving AWS Secrets Manager references by full ARN
 *       and by {@code aws-sm://name}.</li>
 *   <li>{@code GcpFederatedAwsCredentialsProvider}'s STS {@code AssumeRoleWithWebIdentity}
 *       exchange (the GCP ID-token minting is stubbed — LocalStack accepts any token).</li>
 * </ul>
 */
@Testcontainers
public class AwsAuthIT {

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.4"))
            .withServices(
                    LocalStackContainer.Service.S3,
                    LocalStackContainer.Service.SECRETSMANAGER,
                    LocalStackContainer.Service.STS);

    private static PipelineOptions applyAwsOptions() {
        final Options options = JsonUtil.fromJson("""
                {
                  "aws": {
                    "region": "%s",
                    "endpoint": "%s",
                    "credentials": {
                      "accessKey": "%s",
                      "secretKey": "%s"
                    }
                  }
                }
                """.formatted(
                        localstack.getRegion(),
                        localstack.getEndpoint(),
                        localstack.getAccessKey(),
                        localstack.getSecretKey()),
                Options.class);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        // registers FileSystems (s3://) and SecretProviders with these options
        Options.setOptions(pipelineOptions, options);
        return pipelineOptions;
    }

    @Test
    public void testS3ReadWriteViaResourceUtil() {
        final PipelineOptions pipelineOptions = applyAwsOptions();

        // schema-sampling client goes through the same S3ClientBuilderFactory chain as runtime IO
        try(final S3Client s3 = S3Util.storage(pipelineOptions)) {
            s3.createBucket(b -> b.bucket("resource-util-test"));
        }

        final String path = "s3://resource-util-test/dir/test.txt";
        Assertions.assertFalse(ResourceUtil.exists(path));
        ResourceUtil.writeString(path, "hello s3");
        Assertions.assertTrue(ResourceUtil.exists(path));
        Assertions.assertEquals("hello s3", ResourceUtil.readString(path));
    }

    @Test
    public void testSecretsManagerResolution() {
        applyAwsOptions();

        try(final SecretsManagerClient client = SecretsManagerClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build()) {
            final CreateSecretResponse created = client
                    .createSecret(b -> b.name("db-password").secretString("s3cr3t"));

            Assertions.assertEquals("s3cr3t", SecretProviders.resolve(created.arn()));
            Assertions.assertEquals("s3cr3t", SecretProviders.resolve("aws-sm://db-password"));
            Assertions.assertEquals("s3cr3t", SecretProviders.resolveIfSecret(created.arn()));
        }
    }

    @Test
    public void testGcpFederatedStsExchange() {
        final GcpFederatedAwsCredentialsProvider provider = new GcpFederatedAwsCredentialsProvider(
                "arn:aws:iam::000000000000:role/pipeline-role",
                "test-audience",
                null,
                null,
                localstack.getRegion(),
                localstack.getEndpoint().toString()) {
            @Override
            protected String mintGcpIdToken() {
                return "stub-gcp-id-token";
            }
        };

        final AwsCredentials credentials = provider.resolveCredentials();
        Assertions.assertNotNull(credentials.accessKeyId());
        Assertions.assertNotNull(credentials.secretAccessKey());
        // second call within the expiry margin returns the cached session credentials
        Assertions.assertSame(credentials, provider.resolveCredentials());
    }

}
