package com.mercari.solution.util.domain.text.template;

import com.mercari.solution.util.cloud.SecretProviders;

/**
 * Cloud-neutral secret resolution for templates: {@code ${utils.secrets.get("...")}}.
 * Accepts any reference syntax supported by {@link SecretProviders} (GCP Secret Manager,
 * AWS Secrets Manager, Vault); a non-reference value is returned unchanged.
 * Prefer this over the GCP-only {@code utils.gcp.secret(...)}.
 */
public class SecretFunctions {

    public String get(final String reference) {
        return SecretProviders.resolveIfSecret(reference);
    }

}
