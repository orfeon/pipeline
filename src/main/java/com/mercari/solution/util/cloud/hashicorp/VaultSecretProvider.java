package com.mercari.solution.util.cloud.hashicorp;

import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.SecretProvider;

/**
 * HashiCorp Vault backend: {@code vault://v1/{kv-path}#{field}}
 * (e.g. {@code vault://v1/secret/data/myapp#password}).
 *
 * Connection and auth settings come from the environment (or system properties):
 * {@code VAULT_ADDR} (required), {@code VAULT_NAMESPACE}, and {@code VAULT_AUTH} —
 * one of {@code token} ({@code VAULT_TOKEN}), {@code gcp}
 * ({@code VAULT_AUTH_SERVICE_ACCOUNT} + {@code VAULT_ROLE}), or {@code aws-iam}
 * ({@code VAULT_ROLE} + optional {@code VAULT_AWS_IAM_SERVER_ID}, signs with the AWS
 * default credentials chain). When {@code VAULT_AUTH} is unset it is inferred:
 * {@code token} if {@code VAULT_TOKEN} is set, {@code gcp} if
 * {@code VAULT_AUTH_SERVICE_ACCOUNT} is set.
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

        final VaultClient client = createClient(addr);
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

    private static VaultClient createClient(final String addr) {
        final String namespace = env("VAULT_NAMESPACE");
        String auth = env("VAULT_AUTH");
        if(auth == null) {
            if(env("VAULT_TOKEN") != null) {
                auth = "token";
            } else if(env("VAULT_AUTH_SERVICE_ACCOUNT") != null) {
                auth = "gcp";
            } else {
                throw new IllegalStateException(
                        "Set VAULT_AUTH to one of [gcp, aws-iam, token] to resolve vault:// secret references"
                                + " (or set VAULT_TOKEN / VAULT_AUTH_SERVICE_ACCOUNT to infer it)");
            }
        }
        return switch (auth) {
            case "token" -> VaultClient.withToken(addr, namespace, requireEnv("VAULT_TOKEN"));
            case "gcp" -> VaultClient.withGcpAuth(
                    addr, requireEnv("VAULT_AUTH_SERVICE_ACCOUNT"), namespace, env("VAULT_ROLE"), null);
            case "aws-iam" -> VaultClient.withAwsIamAuth(
                    addr, namespace, env("VAULT_ROLE"), env("VAULT_AWS_IAM_SERVER_ID"), null);
            default -> throw new IllegalStateException(
                    "VAULT_AUTH must be one of [gcp, aws-iam, token] but was: " + auth);
        };
    }

    private static String requireEnv(final String name) {
        final String value = env(name);
        if(value == null) {
            throw new IllegalStateException(name + " must be set to resolve vault:// secret references");
        }
        return value;
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
