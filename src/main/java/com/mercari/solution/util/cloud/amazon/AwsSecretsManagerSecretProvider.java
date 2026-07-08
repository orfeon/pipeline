package com.mercari.solution.util.cloud.amazon;

import com.mercari.solution.util.cloud.SecretProvider;
import com.mercari.solution.util.cloud.SecretProviders;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.Base64;

/**
 * AWS Secrets Manager backend: a full secret ARN ({@code arn:aws:secretsmanager:...}, the region
 * is taken from the ARN) or {@code aws-sm://name} (region from {@code options.aws.region} /
 * the environment).
 *
 * Credentials: the launcher-configured pipeline options ({@code options.aws.credentials},
 * including {@code gcpFederation}); otherwise the AWS default provider chain. Runtime resolution
 * on GCP workers (no configured options) therefore requires ambient AWS credentials — a
 * documented limitation (docs/developer/cloud-auth.md §8).
 */
public class AwsSecretsManagerSecretProvider implements SecretProvider {

    private static final String PREFIX_ARN = "arn:aws:secretsmanager:";
    private static final String PREFIX_NAME = "aws-sm://";

    @Override
    public boolean matches(final String reference) {
        return reference != null
                && (reference.startsWith(PREFIX_ARN) || reference.startsWith(PREFIX_NAME));
    }

    @Override
    public String resolve(final String reference) {
        final String secretId = reference.startsWith(PREFIX_NAME)
                ? reference.substring(PREFIX_NAME.length())
                : reference;
        try(final SecretsManagerClient client = createClient(reference)) {
            final GetSecretValueResponse response = client
                    .getSecretValue(GetSecretValueRequest.builder().secretId(secretId).build());
            if(response.secretString() != null) {
                return response.secretString();
            }
            return Base64.getEncoder().encodeToString(response.secretBinary().asByteArray());
        }
    }

    private static SecretsManagerClient createClient(final String reference) {
        final SecretsManagerClientBuilder builder = SecretsManagerClient.builder();
        final String arnRegion = parseArnRegion(reference);
        final PipelineOptions pipelineOptions = SecretProviders.getPipelineOptions();
        if(pipelineOptions != null) {
            final AwsOptions awsOptions = pipelineOptions.as(AwsOptions.class);
            if(awsOptions.getEndpoint() != null) {
                builder.endpointOverride(awsOptions.getEndpoint());
            }
            if(arnRegion == null && awsOptions.getAwsRegion() != null) {
                builder.region(awsOptions.getAwsRegion());
            }
            final String federationRoleArn = pipelineOptions
                    .as(GcpFederatedAwsCredentialsProvider.FederationOptions.class)
                    .getAwsFederationRoleArn();
            if(federationRoleArn != null) {
                builder.credentialsProvider(GcpFederatedAwsCredentialsProvider.fromOptions(pipelineOptions));
            } else if(awsOptions.getAwsCredentialsProvider() != null) {
                builder.credentialsProvider(awsOptions.getAwsCredentialsProvider());
            }
        }
        if(arnRegion != null) {
            builder.region(Region.of(arnRegion));
        }
        return builder.build();
    }

    // arn:aws:secretsmanager:{region}:{account}:secret:{name}
    public static String parseArnRegion(final String reference) {
        if(!reference.startsWith(PREFIX_ARN)) {
            return null;
        }
        final String[] parts = reference.split(":", 5);
        return parts.length >= 4 && !parts[3].isEmpty() ? parts[3] : null;
    }

}
