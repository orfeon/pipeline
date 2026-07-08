package com.mercari.solution.util.cloud.amazon;

import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

@ResourceLock("GcpCredentialsCache")
public class GcpFederatedAwsCredentialsProviderTest {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/pipeline-role";

    @Test
    public void testDefaults() {
        final GcpFederatedAwsCredentialsProvider provider =
                new GcpFederatedAwsCredentialsProvider(ROLE_ARN, null, null, null, null, null);
        Assertions.assertEquals(ROLE_ARN, provider.getRoleArn());
        Assertions.assertEquals(ROLE_ARN, provider.getAudience());
        Assertions.assertEquals("mercari-pipeline", provider.getRoleSessionName());
    }

    @Test
    public void testRoleArnRequired() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                new GcpFederatedAwsCredentialsProvider(null, null, null, null, "ap-northeast-1", null));
    }

    @Test
    public void testFromOptions() {
        final PipelineOptions options = PipelineOptionsFactory.create();
        final GcpFederatedAwsCredentialsProvider.FederationOptions federation =
                options.as(GcpFederatedAwsCredentialsProvider.FederationOptions.class);
        federation.setAwsFederationRoleArn(ROLE_ARN);
        federation.setAwsFederationAudience("my-audience");
        federation.setAwsFederationRoleSessionName("session");
        final AwsOptions awsOptions = options.as(AwsOptions.class);
        awsOptions.setAwsRegion(Region.AP_NORTHEAST_1);
        awsOptions.setEndpoint(URI.create("http://localhost:4566"));

        final GcpFederatedAwsCredentialsProvider provider = GcpFederatedAwsCredentialsProvider.fromOptions(options);
        Assertions.assertEquals(ROLE_ARN, provider.getRoleArn());
        Assertions.assertEquals("my-audience", provider.getAudience());
        Assertions.assertEquals("session", provider.getRoleSessionName());
    }

    @Test
    public void testJavaSerializable() {
        final GcpFederatedAwsCredentialsProvider provider = new GcpFederatedAwsCredentialsProvider(
                ROLE_ARN, "aud", "session", 900, "ap-northeast-1", null);
        final GcpFederatedAwsCredentialsProvider restored = SerializableUtils.clone(provider);
        Assertions.assertEquals(ROLE_ARN, restored.getRoleArn());
        Assertions.assertEquals("aud", restored.getAudience());
        Assertions.assertEquals("session", restored.getRoleSessionName());
    }

    // Full STS exchange needs a live token issuer + STS; covered by the Phase 5 LocalStack IT.
    // Here: credentials that cannot mint ID tokens must fail with a precise message, offline.
    @Test
    public void testResolveFailsClearlyWithoutIdTokenProvider() {
        GcpCredentialsCache.setSource("classpath:gcp-wif-test-credentials.json");
        final GcpFederatedAwsCredentialsProvider provider = new GcpFederatedAwsCredentialsProvider(
                ROLE_ARN, null, null, null, "ap-northeast-1", null);
        final IllegalStateException e = Assertions
                .assertThrows(IllegalStateException.class, provider::resolveCredentials);
        Assertions.assertTrue(e.getMessage().contains("cannot mint ID tokens"));
    }

}
