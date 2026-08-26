package com.mercari.solution.module.action;

import com.mercari.solution.module.Schema;

import java.io.Serializable;

/**
 * Result of a single {@link Action} execution.
 * The action sink wraps it into the common output envelope
 * ({@code service, operation, jobId, state, startedAt, finishedAt, payload}) defined by {@link #createOutputSchema()},
 * so every action service produces records with the same schema.
 */
public class ActionResult implements Serializable {

    private final String operation;
    private final String jobId;
    private final String state;
    private final String payload;

    private ActionResult(final String operation, final String jobId, final String state, final String payload) {
        this.operation = operation;
        this.jobId = jobId;
        this.state = state;
        this.payload = payload;
    }

    public static ActionResult of(final String operation, final String jobId, final String state, final String payload) {
        return new ActionResult(operation, jobId, state, payload);
    }

    public String getOperation() {
        return operation;
    }

    public String getJobId() {
        return jobId;
    }

    public String getState() {
        return state;
    }

    public String getPayload() {
        return payload;
    }

    public static Schema createOutputSchema() {
        return Schema.builder()
                .withField(Schema.Field.of("service", Schema.FieldType.STRING))
                .withField(Schema.Field.of("operation", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("jobId", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("state", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("startedAt", Schema.FieldType.TIMESTAMP))
                .withField(Schema.Field.of("finishedAt", Schema.FieldType.TIMESTAMP))
                .withField(Schema.Field.of("payload", Schema.FieldType.STRING.withNullable(true)))
                .build();
    }

    @Override
    public String toString() {
        return "ActionResult{operation=" + operation + ", jobId=" + jobId + ", state=" + state + "}";
    }

}
