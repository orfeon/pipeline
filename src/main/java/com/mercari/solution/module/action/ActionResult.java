package com.mercari.solution.module.action;

import com.mercari.solution.module.Schema;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.Map;

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
    /** Typed view of the payload (nested maps/lists, numbers as numbers) for condition evaluation; null when the service supplied only text. */
    private final Map<String, Object> payloadValues;

    private ActionResult(final String operation, final String jobId, final String state, final String payload, final Map<String, Object> payloadValues) {
        this.operation = operation;
        this.jobId = jobId;
        this.state = state;
        this.payload = payload;
        this.payloadValues = payloadValues;
    }

    public static ActionResult of(final String operation, final String jobId, final String state, final String payload) {
        return new ActionResult(operation, jobId, state, payload, null);
    }

    /**
     * Result whose payload is a structured value: the envelope carries it serialized as JSON and
     * module-level {@code failWhen} / {@code skipWhen} conditions evaluate against the typed map
     * (so numeric fields compare as numbers).
     */
    public static ActionResult ofValues(final String operation, final String jobId, final String state, final Map<String, Object> payloadValues) {
        final String payload = payloadValues == null ? null : new Gson().toJson(payloadValues);
        return new ActionResult(operation, jobId, state, payload, payloadValues);
    }

    /** The same result with another state (e.g. {@code SKIPPED} when a skipWhen condition matched). */
    public ActionResult withState(final String state) {
        return new ActionResult(operation, jobId, state, payload, payloadValues);
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

    public Map<String, Object> getPayloadValues() {
        return payloadValues;
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
