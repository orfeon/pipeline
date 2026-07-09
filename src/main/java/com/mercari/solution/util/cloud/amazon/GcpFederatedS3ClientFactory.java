package com.mercari.solution.util.cloud.amazon;

import org.apache.beam.sdk.io.aws2.options.S3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.sdk.io.aws2.s3.DefaultS3ClientBuilderFactory;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * S3 client factory for {@code credentials.type: gcpFederation}: Beam's default builder wiring
 * (region, endpoint, ...) with credentials swapped for {@link GcpFederatedAwsCredentialsProvider}.
 * Set as {@code S3Options.s3ClientFactoryClass} — a class-name option that serializes to workers
 * where a custom AwsCredentialsProvider in {@code AwsOptions} would not (AwsModule limitation,
 * docs/developer/cloud-auth.md §5.2).
 */
public class GcpFederatedS3ClientFactory implements S3ClientBuilderFactory {

    @Override
    public S3ClientBuilder createBuilder(final S3Options s3Options) {
        return new DefaultS3ClientBuilderFactory()
                .createBuilder(s3Options)
                .credentialsProvider(GcpFederatedAwsCredentialsProvider.fromOptions(s3Options));
    }

}
