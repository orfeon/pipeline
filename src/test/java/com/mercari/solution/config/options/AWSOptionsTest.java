package com.mercari.solution.config.options;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mercari.solution.config.Options;
import com.mercari.solution.util.domain.file.JsonUtil;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;

public class AWSOptionsTest {

    private static AwsOptions applyOptions(final String optionsJson) {
        final Options options = JsonUtil.fromJson(optionsJson, Options.class);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Options.setOptions(pipelineOptions, options);
        return pipelineOptions.as(AwsOptions.class);
    }

    @Test
    public void testRegionAndEndpoint() {
        final AwsOptions awsOptions = applyOptions("""
                {
                  "aws": {
                    "region": "ap-northeast-1",
                    "endpoint": "http://localhost:4566"
                  }
                }
                """);
        Assertions.assertEquals(Region.AP_NORTHEAST_1, awsOptions.getAwsRegion());
        Assertions.assertEquals(URI.create("http://localhost:4566"), awsOptions.getEndpoint());
        // no credentials block: Beam's default factory applies (default provider chain)
        Assertions.assertInstanceOf(DefaultCredentialsProvider.class, awsOptions.getAwsCredentialsProvider());
    }

    @Test
    public void testStaticCredentialsTypeInferred() {
        final AwsOptions awsOptions = applyOptions("""
                {
                  "aws": {
                    "region": "us-east-1",
                    "credentials": {
                      "accessKey": "AKIAEXAMPLE",
                      "secretKey": "secret"
                    }
                  }
                }
                """);
        final AwsCredentialsProvider provider = awsOptions.getAwsCredentialsProvider();
        Assertions.assertInstanceOf(StaticCredentialsProvider.class, provider);
        final AwsCredentials credentials = provider.resolveCredentials();
        Assertions.assertEquals("AKIAEXAMPLE", credentials.accessKeyId());
        Assertions.assertEquals("secret", credentials.secretAccessKey());
    }

    @Test
    public void testStaticCredentialsWithSessionToken() {
        final AwsOptions awsOptions = applyOptions("""
                {
                  "aws": {
                    "credentials": {
                      "type": "static",
                      "accessKey": "AKIAEXAMPLE",
                      "secretKey": "secret",
                      "sessionToken": "token"
                    }
                  }
                }
                """);
        final AwsCredentials credentials = awsOptions.getAwsCredentialsProvider().resolveCredentials();
        Assertions.assertInstanceOf(AwsSessionCredentials.class, credentials);
        Assertions.assertEquals("token", ((AwsSessionCredentials) credentials).sessionToken());
    }

    @Test
    public void testAssumeRoleTypeInferred() {
        final AwsOptions awsOptions = applyOptions("""
                {
                  "aws": {
                    "region": "ap-northeast-1",
                    "credentials": {
                      "roleArn": "arn:aws:iam::123456789012:role/pipeline-role",
                      "externalId": "ext",
                      "durationSeconds": 900
                    }
                  }
                }
                """);
        Assertions.assertInstanceOf(StsAssumeRoleCredentialsProvider.class, awsOptions.getAwsCredentialsProvider());
    }

    @Test
    public void testExplicitDefaultType() {
        final AwsOptions awsOptions = applyOptions("""
                {
                  "aws": {
                    "credentials": {
                      "type": "default"
                    }
                  }
                }
                """);
        Assertions.assertInstanceOf(DefaultCredentialsProvider.class, awsOptions.getAwsCredentialsProvider());
    }

    @Test
    public void testUnknownTypeThrows() {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
                applyOptions("""
                        {
                          "aws": {
                            "credentials": {
                              "type": "gcpFederation"
                            }
                          }
                        }
                        """));
        Assertions.assertTrue(e.getMessage().contains("options.aws.credentials.type"));
    }

    @Test
    public void testStaticMissingSecretKeyThrows() {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
                applyOptions("""
                        {
                          "aws": {
                            "credentials": {
                              "type": "static",
                              "accessKey": "AKIAEXAMPLE"
                            }
                          }
                        }
                        """));
        Assertions.assertTrue(e.getMessage().contains("static"));
    }

    @Test
    public void testAssumeRoleMissingRoleArnThrows() {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
                applyOptions("""
                        {
                          "aws": {
                            "credentials": {
                              "type": "assumeRole"
                            }
                          }
                        }
                        """));
        Assertions.assertTrue(e.getMessage().contains("roleArn"));
    }

    // Beam serializes AwsOptions.awsCredentialsProvider to workers with Jackson via AwsModule,
    // which supports only a fixed set of provider types (docs/developer/cloud-auth.md §5.1).
    // Guard that every provider type we emit survives that serialization.
    @Test
    public void testEmittedProvidersAreBeamSerializable() throws Exception {
        final ObjectMapper mapper = new ObjectMapper()
                .registerModules(ObjectMapper.findModules());

        final AwsOptions staticOptions = applyOptions("""
                {
                  "aws": {
                    "credentials": {
                      "accessKey": "AKIAEXAMPLE",
                      "secretKey": "secret"
                    }
                  }
                }
                """);
        final String staticJson = mapper.writeValueAsString(staticOptions.getAwsCredentialsProvider());
        final AwsCredentialsProvider restored = mapper.readValue(staticJson, AwsCredentialsProvider.class);
        Assertions.assertEquals("AKIAEXAMPLE", restored.resolveCredentials().accessKeyId());

        final AwsOptions assumeRoleOptions = applyOptions("""
                {
                  "aws": {
                    "region": "ap-northeast-1",
                    "credentials": {
                      "roleArn": "arn:aws:iam::123456789012:role/pipeline-role"
                    }
                  }
                }
                """);
        final String assumeRoleJson = mapper.writeValueAsString(assumeRoleOptions.getAwsCredentialsProvider());
        Assertions.assertTrue(assumeRoleJson.contains("arn:aws:iam::123456789012:role/pipeline-role"));
    }

}
