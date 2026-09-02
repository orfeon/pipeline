package com.mercari.solution.module.action;

/**
 * Thrown by an {@link ActionService} for a failure that re-executing the same firing cannot fix
 * (rejected request, failed terminal state, template error): the module-level {@code retry} does
 * not re-attempt it and routes the firing to failure handling immediately.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(final String message) {
        super(message);
    }

    public NonRetryableException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
