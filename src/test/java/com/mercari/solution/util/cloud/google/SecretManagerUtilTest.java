package com.mercari.solution.util.cloud.google;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SecretManagerUtilTest {

    @Test
    public void testIsSecretName() {
        final String secretName1 = "projects/my-project1/secrets/my-secrets_/versions/1";
        Assertions.assertTrue(SecretManagerUtil.isSecretName(secretName1));
        final String secretName2 = "/projects/my-project1/secrets/my-secrets_/versions/1";
        Assertions.assertFalse(SecretManagerUtil.isSecretName(secretName2));
        final String secretName3 = "projects//secrets/my-secrets_/versions/1";
        Assertions.assertFalse(SecretManagerUtil.isSecretName(secretName3));
        final String secretName4 = "projects/my-project1/secrets//versions/1";
        Assertions.assertFalse(SecretManagerUtil.isSecretName(secretName4));
        final String secretName5 = "projects/my-project1/secrets/my-secrets/versions/";
        Assertions.assertFalse(SecretManagerUtil.isSecretName(secretName5));
        final String secretName6 = "projects/my-project1/secrets/my-secrets/versions/a";
        Assertions.assertFalse(SecretManagerUtil.isSecretName(secretName6));
        final String secretName7 = "projects/my-project1/secrets/my-secrets/versions/latest";
        Assertions.assertTrue(SecretManagerUtil.isSecretName(secretName7));
    }

}
