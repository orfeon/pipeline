package com.mercari.solution.util.cloud;

import com.mercari.solution.util.cloud.amazon.AwsSecretsManagerSecretProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class SecretProvidersTest {

    @Test
    public void testReferenceDispatch() {
        Assertions.assertTrue(SecretProviders
                .isSecretReference("projects/myproject/secrets/mysecret/versions/latest"));
        Assertions.assertTrue(SecretProviders
                .isSecretReference("arn:aws:secretsmanager:ap-northeast-1:123456789012:secret:db-password-AbCdEf"));
        Assertions.assertTrue(SecretProviders.isSecretReference("aws-sm://db-password"));
        Assertions.assertTrue(SecretProviders.isSecretReference("vault://v1/secret/data/myapp#password"));

        Assertions.assertFalse(SecretProviders.isSecretReference("plain-password"));
        Assertions.assertFalse(SecretProviders.isSecretReference("projects/myproject/topics/mytopic"));
        Assertions.assertFalse(SecretProviders.isSecretReference(null));
    }

    @Test
    public void testResolveIfSecretPassesThroughPlainValues() {
        Assertions.assertEquals("plain-password", SecretProviders.resolveIfSecret("plain-password"));
        Assertions.assertNull(SecretProviders.resolveIfSecret(null));
    }

    @Test
    public void testResolveRejectsNonReference() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SecretProviders.resolve("plain-password"));
    }

    @Test
    public void testAwsArnRegionParsing() {
        Assertions.assertEquals("ap-northeast-1", AwsSecretsManagerSecretProvider
                .parseArnRegion("arn:aws:secretsmanager:ap-northeast-1:123456789012:secret:db-password-AbCdEf"));
        Assertions.assertNull(AwsSecretsManagerSecretProvider.parseArnRegion("aws-sm://db-password"));
    }

    @Test
    public void testVaultRequiresAddress() {
        Assumptions.assumeTrue(System.getenv("VAULT_ADDR") == null);
        final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, () ->
                SecretProviders.resolve("vault://v1/secret/data/myapp#password"));
        Assertions.assertTrue(e.getMessage().contains("VAULT_ADDR"));
    }

}
