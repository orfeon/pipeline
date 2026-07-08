package com.mercari.solution.util.cloud.hashicorp;

import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.SecretProvider;

/**
 * HashiCorp Vault backend: {@code vault://v1/{kv-path}#{field}}
 * (e.g. {@code vault://v1/secret/data/myapp#password}).
 *
 * Connection settings come from the environment: {@code VAULT_ADDR} (required),
 * {@code VAULT_NAMESPACE}, {@code VAULT_ROLE}, and {@code VAULT_AUTH_SERVICE_ACCOUNT}
 * (the GCP service account for Vault's gcp auth backend — the only auth method
 * {@link VaultClient} supports today; alternatives are scheduled for Phase 5).
 */
public class VaultSecretProvider implements SecretProvider {

    private static final String PREFIX = "vault://";

    @Override
    public boolean matches(final String reference) {
        return reference != null && reference.startsWith(PREFIX);
    }

    @Override
    public String resolve(final String reference) {
        final String addr = env("VAULT_ADDR");
        if(addr == null) {
            throw new IllegalStateException(
                    "VAULT_ADDR must be set to resolve vault:// secret references: " + reference);
        }

        final String pathAndField = reference.substring(PREFIX.length());
        final int sharp = pathAndField.lastIndexOf('#');
        final String path = sharp < 0 ? pathAndField : pathAndField.substring(0, sharp);
        final String field = sharp < 0 ? null : pathAndField.substring(sharp + 1);

        final VaultClient client = new VaultClient(
                addr, env("VAULT_AUTH_SERVICE_ACCOUNT"), env("VAULT_NAMESPACE"), env("VAULT_ROLE"), null);
        try {
            final JsonObject data = client.readKVSecret("/" + path);
            if(field != null) {
                if(!data.has(field)) {
                    throw new IllegalArgumentException(
                            "Vault secret " + path + " has no field '" + field + "' (fields: " + data.keySet() + ")");
                }
                return data.get(field).getAsString();
            }
            if(data.size() == 1) {
                return data.entrySet().iterator().next().getValue().getAsString();
            }
            throw new IllegalArgumentException(
                    "Vault secret " + path + " has multiple fields " + data.keySet()
                            + "; specify one as vault://" + path + "#{field}");
        } catch (final IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to read Vault secret: " + reference, e);
        } finally {
            revokeQuietly(client);
        }
    }

    private static void revokeQuietly(final VaultClient client) {
        try {
            client.revokeToken();
        } catch (final Exception e) {
            // token expires on its own; revocation is best-effort
        }
    }

    private static String env(final String name) {
        return System.getProperty(name, System.getenv(name));
    }

}
