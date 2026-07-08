package com.mercari.solution.util.cloud.google;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AwsCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.mercari.solution.config.Options;
import com.mercari.solution.util.domain.file.JsonUtil;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.nio.file.Files;
import java.nio.file.Path;

// Serialized: GcpCredentialsCache holds static state shared across these tests
// (and with other @ResourceLock("GcpCredentialsCache") classes).
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("GcpCredentialsCache")
public class GcpCredentialsCacheTest {

    // A workload identity federation config contains no key material (audience/URL references
    // only), which is why it may be embedded inline or bundled in the application jar.
    private static String wifJson(final String poolId) {
        return """
                {
                  "type": "external_account",
                  "audience": "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/%s/providers/aws-test",
                  "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
                  "token_url": "https://sts.googleapis.com/v1/token",
                  "credential_source": {
                    "environment_id": "aws1",
                    "region_url": "http://169.254.169.254/latest/meta-data/placement/availability-zone",
                    "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials",
                    "regional_cred_verification_url": "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
                  }
                }
                """.formatted(poolId);
    }

    @Test
    public void testInlineJsonSource() throws Exception {
        GcpCredentialsCache.setSource(wifJson("inline-pool"));
        final GoogleCredentials credentials = GcpCredentialsCache.credentials();
        Assertions.assertInstanceOf(AwsCredentials.class, credentials);
        // cached until the source changes
        Assertions.assertSame(credentials, GcpCredentialsCache.credentials());
    }

    @Test
    public void testClasspathSource() throws Exception {
        GcpCredentialsCache.setSource("classpath:gcp-wif-test-credentials.json");
        Assertions.assertInstanceOf(AwsCredentials.class, GcpCredentialsCache.credentials());
    }

    @Test
    public void testFilePathSource(@TempDir final Path tempDir) throws Exception {
        final Path file = tempDir.resolve("wif.json");
        Files.writeString(file, wifJson("file-pool"));
        GcpCredentialsCache.setSource(file.toString());
        Assertions.assertInstanceOf(AwsCredentials.class, GcpCredentialsCache.credentials());
    }

    @Test
    public void testSourceSwitchResetsCache() throws Exception {
        GcpCredentialsCache.setSource(wifJson("pool-a"));
        final GoogleCredentials a = GcpCredentialsCache.credentials();
        GcpCredentialsCache.setSource(wifJson("pool-b"));
        final GoogleCredentials b = GcpCredentialsCache.credentials();
        Assertions.assertNotSame(a, b);
    }

    @Test
    public void testCredentialFactoryWiring() {
        final Options options = JsonUtil.fromJson("""
                {
                  "gcp": {
                    "credentials": "classpath:gcp-wif-test-credentials.json"
                  }
                }
                """, Options.class);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Options.setOptions(pipelineOptions, options);

        Assertions.assertEquals(
                MCredentialFactory.class,
                pipelineOptions.as(GcpOptions.class).getCredentialFactoryClass());
        Assertions.assertEquals(
                "classpath:gcp-wif-test-credentials.json",
                pipelineOptions.as(MCredentialFactory.CredentialsSourceOptions.class).getGcpCredentialsSource());

        final Credentials credentials = MCredentialFactory.fromOptions(pipelineOptions).getCredential();
        Assertions.assertInstanceOf(AwsCredentials.class, credentials);
    }

    @Test
    public void testMetadataGuardOffGcp() {
        Assumptions.assumeFalse(IAMUtil.isOnGcp());
        final long start = System.currentTimeMillis();
        Assertions.assertNull(IAMUtil.getMetadataProject());
        Assertions.assertNull(IAMUtil.getMetadataServiceAccount());
        // guarded lookups return immediately instead of retrying against the metadata server
        Assertions.assertTrue(System.currentTimeMillis() - start < 5000);
    }

}
