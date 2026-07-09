package com.mercari.solution.util.cloud.google;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A Beam {@link CredentialFactory} backed by {@link GcpCredentialsCache}, so that Beam's GCP IOs
 * (BigQueryIO, SpannerIO, ...) and this project's client utils share one credential source.
 *
 * Activated by {@code options.gcp.credentials}: {@code GCPOptions.setOptions} stores the source
 * in {@link CredentialsSourceOptions} (serialized to workers with the pipeline options) and sets
 * this class as {@code GcpOptions.credentialFactoryClass}. Mirrors Beam's GcpCredentialFactory
 * scoping/impersonation behavior. See docs/developer/cloud-auth.md §4.1.
 */
public class MCredentialFactory implements CredentialFactory {

    public interface CredentialsSourceOptions extends PipelineOptions {
        String getGcpCredentialsSource();
        void setGcpCredentialsSource(String gcpCredentialsSource);
    }

    private final List<String> oauthScopes;
    private final String impersonateServiceAccount;
    private final String credentialsSource;

    private MCredentialFactory(
            final List<String> oauthScopes,
            final String impersonateServiceAccount,
            final String credentialsSource) {

        this.oauthScopes = oauthScopes;
        this.impersonateServiceAccount = impersonateServiceAccount;
        this.credentialsSource = credentialsSource;
    }

    public static MCredentialFactory fromOptions(final PipelineOptions options) {
        final GcpOptions gcpOptions = options.as(GcpOptions.class);
        return new MCredentialFactory(
                gcpOptions.getGcpOauthScopes(),
                gcpOptions.getImpersonateServiceAccount(),
                options.as(CredentialsSourceOptions.class).getGcpCredentialsSource());
    }

    @Override
    public Credentials getCredential() {
        try {
            if(credentialsSource != null) {
                GcpCredentialsCache.setSource(credentialsSource);
            }
            GoogleCredentials credentials = GcpCredentialsCache.credentials().createScoped(oauthScopes);
            if(impersonateServiceAccount != null) {
                final List<String> chain = Arrays.asList(impersonateServiceAccount.split(","));
                final String targetPrincipal = chain.get(chain.size() - 1);
                final List<String> delegationChain = chain.subList(0, chain.size() - 1);
                credentials = ImpersonatedCredentials.create(
                        credentials, targetPrincipal, delegationChain, oauthScopes, 3600);
            }
            return credentials;
        } catch (final IOException e) {
            // Beam's GcpCredentialFactory contract: null means "credentials unavailable",
            // letting pipelines that never touch GCP services proceed.
            return null;
        }
    }

}
