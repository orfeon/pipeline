package com.mercari.solution.module.action;

import com.google.common.reflect.ClassPath;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A single operation against an external service, executed from inside a pipeline by the
 * action modules ({@code action.<service>}, placeable in sources/transforms/sinks — e.g.
 * run a BigQuery job, launch a Vertex AI batch prediction job, write a result-history file).
 *
 * Implementations are discovered by scanning this package for classes annotated with
 * {@link Service} — no manual registration, the same convention as pipeline modules.
 *
 * Lifecycle:
 * <ol>
 *   <li>{@link #configure(String, JsonObject, PipelineOptions)} at pipeline assembly time —
 *       deserialize the (flat) parameters object, validate it (throw {@link IllegalModuleException}
 *       on invalid config) and apply defaults. The instance is then serialized into the DoFn,
 *       so all remaining state must be {@link Serializable} (or transient).</li>
 *   <li>{@link #setup()} once per DoFn instance on the worker.</li>
 *   <li>{@link #execute(List)} per trigger firing. The elements list depends on the
 *       {@link Trigger}: empty for {@code once} (pure signal), a single element for
 *       {@code perElement}, and all gathered input elements for {@code collect}.
 *       Services may use the elements to expand templates in their parameters.</li>
 * </ol>
 *
 * Execution guarantee is at-least-once: Beam may retry a bundle, re-invoking {@code execute}
 * for an already-attempted firing. Services should therefore act idempotently where the
 * backing API allows it (e.g. deterministic BigQuery job ids), and document it where it does not.
 */
public interface Action extends Serializable {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface Service {
        String name();
    }

    /**
     * Firing semantics of an action step, given in the flat parameters as {@code trigger}.
     * <ul>
     *   <li>{@code once} (default) — fire exactly once after every input and wait completes;
     *       inputs act purely as completion signals and no element is delivered.</li>
     *   <li>{@code perElement} — fire once per input element.</li>
     *   <li>{@code collect} — gather all input elements and fire once with the full list.
     *       The elements are materialized on a single worker: intended for control records
     *       (file lists, job results), not large data.</li>
     * </ul>
     */
    enum Trigger {
        once,
        perElement,
        collect;

        public static Trigger of(final JsonObject parameters) {
            if(parameters == null || !parameters.has("trigger") || parameters.get("trigger").isJsonNull()) {
                return once;
            }
            final String value = parameters.get("trigger").getAsString();
            try {
                return valueOf(value);
            } catch (final IllegalArgumentException e) {
                throw new IllegalModuleException(
                        "Illegal trigger: " + value + ". supported values: once, perElement, collect");
            }
        }
    }

    void configure(String name, JsonObject parameters, PipelineOptions options);

    /**
     * Variant that also receives the union schema of the step's inputs (null when the step has no
     * inputs, e.g. trigger once without inputs). Services that compile element templates at
     * assembly time override this; the default ignores the schema.
     */
    default void configure(String name, JsonObject parameters, PipelineOptions options, Schema inputSchema) {
        configure(name, parameters, options);
    }

    void setup();

    ActionResult execute(List<MElement> elements) throws Exception;

    static Action create(
            final String name,
            final String service,
            final JsonObject parameters,
            final PipelineOptions options) {
        return create(name, service, parameters, options, null);
    }

    static Action create(
            final String name,
            final String service,
            final JsonObject parameters,
            final PipelineOptions options,
            final Schema inputSchema) {

        final Class<? extends Action> clazz = Registry.SERVICES.get(service);
        if(clazz == null) {
            throw new IllegalModuleException(name, "action",
                    "Not supported action service: " + service + ". supported services: " + serviceNames());
        }
        final Action action;
        try {
            action = clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("Failed to instantiate action service: " + service + ", class: " + clazz, e);
        }
        action.configure(name, parameters, options, inputSchema);
        return action;
    }

    static Set<String> serviceNames() {
        return Registry.SERVICES.keySet();
    }

    /**
     * Template context for {@code collect} trigger templates: exposes {@code elements}
     * (the list of element field maps) and {@code size}.
     */
    static Map<String, Object> createCollectTemplateData(final List<MElement> elements) {
        final List<Map<String, Object>> maps = elements.stream()
                .map(MElement::asPrimitiveMap)
                .toList();
        return Map.of("elements", maps, "size", maps.size());
    }

    final class Registry {

        private static final Map<String, Class<? extends Action>> SERVICES =
                findActionsInPackage(Action.class.getPackageName());

        private Registry() {
        }

        private static Map<String, Class<? extends Action>> findActionsInPackage(final String packageName) {
            final ClassPath classPath;
            try {
                classPath = ClassPath.from(Action.class.getClassLoader());
            } catch (IOException ioe) {
                throw new RuntimeException("Reading classpath resource failed", ioe);
            }
            return classPath.getTopLevelClassesRecursive(packageName)
                    .stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(clazz -> clazz.isAnnotationPresent(Service.class))
                    .peek(clazz -> {
                        if(!Action.class.isAssignableFrom(clazz)) {
                            throw new IllegalArgumentException(
                                    "action service: " + clazz.getName() + " with @Action.Service must implement Action");
                        }
                    })
                    .map(clazz -> clazz.asSubclass(Action.class))
                    .collect(Collectors.toMap(
                            c -> c.getAnnotation(Service.class).name(),
                            c -> c));
        }

    }

}
