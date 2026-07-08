package com.mercari.solution.util.cloud.google;

import com.mercari.solution.util.cloud.SecretProvider;

/** GCP Secret Manager backend: {@code projects/{p}/secrets/{s}/versions/{v}}. */
public class GcpSecretManagerSecretProvider implements SecretProvider {

    @Override
    public boolean matches(final String reference) {
        return SecretManagerUtil.isSecretName(reference);
    }

    @Override
    public String resolve(final String reference) {
        return SecretManagerUtil.getSecret(reference).toStringUtf8();
    }

}
