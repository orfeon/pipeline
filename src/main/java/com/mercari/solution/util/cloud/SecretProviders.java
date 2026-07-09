package com.mercari.solution.util.cloud;

import com.mercari.solution.util.cloud.amazon.AwsSecretsManagerSecretProvider;
import com.mercari.solution.util.cloud.google.GcpSecretManagerSecretProvider;
import com.mercari.solution.util.cloud.hashicorp.VaultSecretProvider;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.List;

/**
 * Cloud-neutral secret resolution, dispatched by reference syntax
 * (docs/developer/cloud-auth.md §6.1):
 *
 * <ul>
 *   <li>GCP Secret Manager — {@code projects/{p}/secrets/{s}/versions/{v}}</li>
 *   <li>AWS Secrets Manager — {@code arn:aws:secretsmanager:...} or {@code aws-sm://name}</li>
 *   <li>HashiCorp Vault — {@code vault://v1/{kv-path}#{field}}</li>
 * </ul>
 */
public class SecretProviders {

    private static final List<SecretProvider> PROVIDERS = List.of(
            new GcpSecretManagerSecretProvider(),
            new AwsSecretsManagerSecretProvider(),
            new VaultSecretProvider());

    // Set on the launcher by Options.setOptions so that resolution sees the configured
    // options.aws (region/endpoint/credentials incl. gcpFederation). On workers this is
    // usually unset and providers fall back to ambient credentials.
    private static volatile PipelineOptions pipelineOptions;

    public static void configure(final PipelineOptions options) {
        pipelineOptions = options;
    }

    public static PipelineOptions getPipelineOptions() {
        return pipelineOptions;
    }

    public static boolean isSecretReference(final String reference) {
        if(reference == null) {
            return false;
        }
        return PROVIDERS.stream().anyMatch(p -> p.matches(reference));
    }

    public static String resolve(final String reference) {
        for(final SecretProvider provider : PROVIDERS) {
            if(provider.matches(reference)) {
                return provider.resolve(reference);
            }
        }
        throw new IllegalArgumentException("Not a supported secret reference: " + reference);
    }

    /** Resolves the reference when it is a secret reference, otherwise returns it unchanged. */
    public static String resolveIfSecret(final String reference) {
        return isSecretReference(reference) ? resolve(reference) : reference;
    }

}
