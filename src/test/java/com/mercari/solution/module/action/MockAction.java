package com.mercari.solution.module.action;

import com.mercari.solution.module.Action;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Action.Trigger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.TemplateUtil;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Test-only action service, discovered by the {@code @Action.Service} classpath scan because
 * test classes live in the same package tree. Echoes its (optionally templated) message as the
 * result payload, or fails when {@code fail} is set — letting module-level tests exercise
 * trigger semantics (once/perElement/collect), the output envelope and failure
 * routing without any external service.
 */
@Action.Service(name = "mock")
public class MockAction implements ActionService {

    public static class Parameters implements Serializable {
        public String message;
        public Boolean fail;
        /** Throw NonRetryableException instead of a retryable failure. */
        public Boolean nonRetryable;
        /** Fail the first N executions of this step (counted in-process), then succeed — for retry tests. */
        public Integer failTimes;
    }

    /** In-process execution counter per step name (the DoFn is deserialized per bundle, so instance fields reset). */
    public static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger> EXECUTIONS =
            new java.util.concurrent.ConcurrentHashMap<>();

    private String name;
    private Trigger trigger;
    private Parameters parameters;

    @Override
    public void configure(final String name, final Trigger trigger, final String operation, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.trigger = trigger;
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null || this.parameters.message == null) {
            throw new IllegalModuleException("action module[" + name + "].parameters.message must not be null");
        }
        if(this.parameters.fail == null) {
            this.parameters.fail = false;
        }
        if(this.parameters.nonRetryable == null) {
            this.parameters.nonRetryable = false;
        }
        if(this.parameters.failTimes == null) {
            this.parameters.failTimes = 0;
        }
    }

    @Override
    public void setup() {

    }

    @Override
    public ActionResult execute(final List<MElement> elements) {
        final int execution = EXECUTIONS.computeIfAbsent(name, n -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
        if(parameters.fail || execution <= parameters.failTimes) {
            if(parameters.nonRetryable) {
                throw new NonRetryableException("mock action failed permanently (execution " + execution + ")");
            }
            throw new IllegalStateException("mock action failed (execution " + execution + ")");
        }
        final String message;
        if(TemplateUtil.isTemplateText(parameters.message)) {
            final Map<String, Object> data = switch (trigger) {
                case perElement -> elements.getFirst().asPrimitiveMap();
                case once, collect -> Action.createCollectTemplateData(elements);
            };
            message = TemplateUtil.executeStrictTemplate(parameters.message, data);
        } else {
            message = parameters.message;
        }
        return ActionResult.of("echo", "mock-job", "DONE", message);
    }

}
