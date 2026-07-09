package com.mercari.solution.util.cloud.hashicorp;

import com.mercari.solution.util.cloud.SecretProviders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.vault.VaultContainer;

/**
 * Integration test (run via maven-failsafe:
 * {@code mvn verify -DskipITs=false -Djib.skip=true -Dit.test=VaultIT})
 * for {@code vault://} secret resolution with token auth against a dev-mode Vault container
 * (docs/developer/cloud-auth.md §6.4). Settings are passed as system properties, which
 * {@code VaultSecretProvider} reads with priority over environment variables.
 */
@Testcontainers
public class VaultIT {

    private static final String ROOT_TOKEN = "test-root-token";

    @Container
    private static final VaultContainer<?> vault = new VaultContainer<>(
            DockerImageName.parse("hashicorp/vault:1.15"))
            .withVaultToken(ROOT_TOKEN)
            .withInitCommand("kv put secret/myapp password=changeme user=admin");

    @Test
    public void testResolveWithTokenAuth() {
        System.setProperty("VAULT_ADDR", vault.getHttpHostAddress());
        System.setProperty("VAULT_AUTH", "token");
        System.setProperty("VAULT_TOKEN", ROOT_TOKEN);
        try {
            Assertions.assertEquals("changeme",
                    SecretProviders.resolve("vault://v1/secret/data/myapp#password"));
            // an externally supplied token must not be revoked by the provider:
            // a second resolution with the same token must still succeed
            Assertions.assertEquals("admin",
                    SecretProviders.resolve("vault://v1/secret/data/myapp#user"));

            // multi-field secret without a #field selector fails with guidance
            final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
                    SecretProviders.resolve("vault://v1/secret/data/myapp"));
            Assertions.assertTrue(e.getMessage().contains("#"));
        } finally {
            System.clearProperty("VAULT_ADDR");
            System.clearProperty("VAULT_AUTH");
            System.clearProperty("VAULT_TOKEN");
        }
    }

}
