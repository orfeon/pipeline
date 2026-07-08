package com.mercari.solution.util.cloud.amazon;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.time.Instant;

/**
 * AWS credentials for pipelines running on GCP, without AWS key material
 * (docs/developer/cloud-auth.md §5.2): mints a GCP service-account ID token (Google is a
 * standard OIDC issuer for AWS IAM) and exchanges it via STS {@code AssumeRoleWithWebIdentity}.
 *
 * Beam's AwsModule cannot serialize custom providers into {@code AwsOptions}, so this provider
 * must never be set there. It reaches workers through {@link GcpFederatedS3ClientFactory}
 * (a class-name option) reading the federation parameters from {@link FederationOptions}
 * (plain string options), both wired by {@code AWSOptions.setOptions} for
 * {@code credentials.type: gcpFederation}.
 */
public class GcpFederatedAwsCredentialsProvider implements AwsCredentialsProvider, Serializable {

    /** Federation parameters carried to workers with the pipeline options. */
    public interface FederationOptions extends PipelineOptions {
        String getAwsFederationRoleArn();
        void setAwsFederationRoleArn(String awsFederationRoleArn);
        String getAwsFederationAudience();
        void setAwsFederationAudience(String awsFederationAudience);
        String getAwsFederationRoleSessionName();
        void setAwsFederationRoleSessionName(String awsFederationRoleSessionName);
        Integer getAwsFederationDurationSeconds();
        void setAwsFederationDurationSeconds(Integer awsFederationDurationSeconds);
    }

    private static final long REFRESH_MARGIN_SECONDS = 120;

    private final String roleArn;
    private final String audience;
    private final String roleSessionName;
    private final Integer durationSeconds;
    private final String stsRegion;
    private final String stsEndpoint;

    private transient volatile AwsSessionCredentials cached;
    private transient volatile Instant expiration;

    public GcpFederatedAwsCredentialsProvider(
            final String roleArn,
            final String audience,
            final String roleSessionName,
            final Integer durationSeconds,
            final String stsRegion,
            final String stsEndpoint) {

        if(roleArn == null) {
            throw new IllegalArgumentException("gcpFederation credentials require roleArn");
        }
        this.roleArn = roleArn;
        this.audience = audience != null ? audience : roleArn;
        this.roleSessionName = roleSessionName != null ? roleSessionName : "mercari-pipeline";
        this.durationSeconds = durationSeconds;
        this.stsRegion = stsRegion;
        this.stsEndpoint = stsEndpoint;
    }

    public static GcpFederatedAwsCredentialsProvider fromOptions(final PipelineOptions options) {
        final FederationOptions federation = options.as(FederationOptions.class);
        final AwsOptions aws = options.as(AwsOptions.class);
        return new GcpFederatedAwsCredentialsProvider(
                federation.getAwsFederationRoleArn(),
                federation.getAwsFederationAudience(),
                federation.getAwsFederationRoleSessionName(),
                federation.getAwsFederationDurationSeconds(),
                aws.getAwsRegion() == null ? null : aws.getAwsRegion().id(),
                aws.getEndpoint() == null ? null : aws.getEndpoint().toString());
    }

    @Override
    public AwsCredentials resolveCredentials() {
        final AwsSessionCredentials c = cached;
        if(c != null && Instant.now().plusSeconds(REFRESH_MARGIN_SECONDS).isBefore(expiration)) {
            return c;
        }
        synchronized (this) {
            if(cached == null || !Instant.now().plusSeconds(REFRESH_MARGIN_SECONDS).isBefore(expiration)) {
                refresh();
            }
            return cached;
        }
    }

    private void refresh() {
        final AssumeRoleWithWebIdentityRequest.Builder request = AssumeRoleWithWebIdentityRequest.builder()
                .roleArn(roleArn)
                .roleSessionName(roleSessionName)
                .webIdentityToken(mintGcpIdToken());
        if(durationSeconds != null) {
            request.durationSeconds(durationSeconds);
        }
        // AssumeRoleWithWebIdentity is an unsigned STS call: the web identity token is the
        // credential, so no AWS credentials may be required to build the client.
        final StsClientBuilder builder = StsClient.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create());
        if(stsRegion != null) {
            builder.region(Region.of(stsRegion));
        }
        if(stsEndpoint != null) {
            builder.endpointOverride(URI.create(stsEndpoint));
        }
        try(final StsClient sts = builder.build()) {
            final AssumeRoleWithWebIdentityResponse response = sts.assumeRoleWithWebIdentity(request.build());
            final software.amazon.awssdk.services.sts.model.Credentials credentials = response.credentials();
            this.cached = AwsSessionCredentials.create(
                    credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
            this.expiration = credentials.expiration();
        }
    }

    private String mintGcpIdToken() {
        try {
            final GoogleCredentials credentials = GcpCredentialsCache.credentials();
            if(!(credentials instanceof IdTokenProvider idTokenProvider)) {
                throw new IllegalStateException(
                        "GCP credentials of type " + credentials.getClass().getSimpleName()
                                + " cannot mint ID tokens for AWS federation."
                                + " gcpFederation requires compute-engine or service-account credentials"
                                + " (e.g. Dataflow worker identity).");
            }
            final IdTokenCredentials idTokenCredentials = IdTokenCredentials.newBuilder()
                    .setIdTokenProvider(idTokenProvider)
                    .setTargetAudience(audience)
                    .build();
            idTokenCredentials.refresh();
            return idTokenCredentials.getIdToken().getTokenValue();
        } catch (final IOException e) {
            throw new RuntimeException("Failed to mint GCP ID token for AWS federation (audience: " + audience + ")", e);
        }
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getAudience() {
        return audience;
    }

    public String getRoleSessionName() {
        return roleSessionName;
    }

}
