package com.mercari.solution.module.action;

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
 * placements, trigger semantics (once/perElement/collect), the output envelope and failure
 * routing without any external service.
 */
@Action.Service(name = "mock")
public class MockAction implements Action {

    public static class Parameters implements Serializable {
        public String message;
        public Boolean fail;
    }

    private Trigger trigger;
    private Parameters parameters;

    @Override
    public void configure(final String name, final JsonObject parametersJson, final PipelineOptions options) {
        this.trigger = Trigger.of(parametersJson);
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null || this.parameters.message == null) {
            throw new IllegalModuleException("action module[" + name + "].parameters.message must not be null");
        }
        if(this.parameters.fail == null) {
            this.parameters.fail = false;
        }
    }

    @Override
    public void setup() {

    }

    @Override
    public ActionResult execute(final List<MElement> elements) {
        if(parameters.fail) {
            throw new IllegalStateException("mock action failed");
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
