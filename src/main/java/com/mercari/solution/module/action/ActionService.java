package com.mercari.solution.module.action;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Action;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.util.List;

/**
 * SPI of an action service: a single operation against an external service, executed from inside
 * a pipeline by the {@link Action} module (config section {@code actions}, {@code module: <service>}).
 *
 * Implementations are discovered by scanning this package for classes annotated with
 * {@link Action.Service} — no manual registration, the same convention as pipeline modules.
 *
 * Lifecycle:
 * <ol>
 *   <li>{@link #configure} at pipeline assembly time — deserialize the parameters object,
 *       validate it (throw {@link com.mercari.solution.module.IllegalModuleException} on invalid
 *       config) and apply defaults. The instance is then serialized into the DoFn, so all
 *       remaining state must be {@link Serializable} (or transient).</li>
 *   <li>{@link #setup()} once per DoFn instance on the worker.</li>
 *   <li>{@link #execute(List)} per trigger firing. The elements list depends on the
 *       {@link Action.Trigger}: empty for {@code once} (pure signal), a single element for
 *       {@code perElement}, and all gathered input elements for {@code collect}.
 *       Services may use the elements to expand templates in their parameters.</li>
 * </ol>
 *
 * Execution guarantee is at-least-once: Beam may retry a bundle, re-invoking {@code execute}
 * for an already-attempted firing. Services should therefore act idempotently where the
 * backing API allows it (e.g. deterministic BigQuery job ids), and document it where it does not.
 */
public interface ActionService extends Serializable {

    /**
     * @param name        the step name (for error messages and derived ids)
     * @param trigger     the step's trigger (module-level {@code trigger} field, defaulted to {@code once})
     * @param operation   the module-level {@code operation} field, already validated against the values
     *                    declared in {@link Action.Service#operations()}; null for single-operation services
     * @param parameters  the service parameters object
     * @param options     pipeline options
     * @param inputSchema the union schema of the step's inputs, or null when the step has no inputs
     *                    (e.g. trigger once without inputs)
     */
    void configure(String name, Action.Trigger trigger, String operation, JsonObject parameters, PipelineOptions options, Schema inputSchema);

    void setup();

    ActionResult execute(List<MElement> elements) throws Exception;

}
