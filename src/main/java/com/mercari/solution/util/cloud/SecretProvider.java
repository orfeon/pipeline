package com.mercari.solution.util.cloud;

/**
 * A secret backend, selected by the syntax of the secret reference
 * (docs/developer/cloud-auth.md §6.1). Implementations are registered in {@link SecretProviders}.
 */
public interface SecretProvider {

    /** Whether this provider handles the given reference (by syntax; no remote call). */
    boolean matches(String reference);

    /** Resolves the reference to the secret value. */
    String resolve(String reference);

}
