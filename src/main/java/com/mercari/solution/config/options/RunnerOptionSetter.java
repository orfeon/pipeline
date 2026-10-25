package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Sets a property of a runner-specific {@link PipelineOptions} interface that is referenced by class name only
 * (the runner jars are on the classpath of their own Maven profile only).
 *
 * <p>The setter is looked up by name, not by parameter type, so a {@code Boolean} config field fits both a
 * {@code boolean} and a {@code Boolean} setter (the two differ across Beam's option interfaces and a
 * {@code getMethod(name, type)} lookup with the wrong one fails at run time). The value is converted to the
 * setter's parameter type: numbers between numeric types, a {@code String} to an enum
 * ({@link IllegalArgumentException} for an unknown constant).
 */
final class RunnerOptionSetter {

    private final Class<? extends PipelineOptions> clazz;
    private final PipelineOptions options;

    private RunnerOptionSetter(final Class<? extends PipelineOptions> clazz, final PipelineOptions options) {
        this.clazz = clazz;
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    static RunnerOptionSetter of(final PipelineOptions pipelineOptions, final String className) throws ClassNotFoundException {
        final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>) Class.forName(className);
        return new RunnerOptionSetter(clazz, pipelineOptions.as(clazz));
    }

    static RunnerOptionSetter of(final PipelineOptions pipelineOptions, final Class<? extends PipelineOptions> clazz) {
        return new RunnerOptionSetter(clazz, pipelineOptions.as(clazz));
    }

    /** Sets {@code property} to {@code value}; a {@code null} value leaves the option alone. */
    RunnerOptionSetter set(final String property, final Object value) {
        if(value == null) {
            return this;
        }
        final Method setter = setter(property);
        final Object converted = convert(property, value, setter.getParameterTypes()[0]);
        try {
            setter.invoke(options, converted);
        } catch (final IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to set pipeline option " + property + " of " + clazz.getSimpleName(), e);
        }
        return this;
    }

    /** Reads {@code property} (via {@code getX} or {@code isX}). */
    Object get(final String property) {
        final String suffix = Character.toUpperCase(property.charAt(0)) + property.substring(1);
        for(final String prefix : new String[]{"get", "is"}) {
            try {
                return clazz.getMethod(prefix + suffix).invoke(options);
            } catch (final NoSuchMethodException e) {
                // try the next prefix
            } catch (final IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Failed to get pipeline option " + property + " of " + clazz.getSimpleName(), e);
            }
        }
        throw new IllegalStateException("Pipeline option " + property + " is not defined in " + clazz.getName());
    }

    private Method setter(final String property) {
        final String name = "set" + Character.toUpperCase(property.charAt(0)) + property.substring(1);
        for(final Method method : clazz.getMethods()) {
            if(method.getName().equals(name) && method.getParameterCount() == 1) {
                return method;
            }
        }
        throw new IllegalStateException("Pipeline option " + property + " is not defined in " + clazz.getName());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Object convert(final String property, final Object value, final Class<?> type) {
        if(type.isEnum() && value instanceof String s) {
            try {
                return Enum.valueOf((Class<? extends Enum>) type, s);
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("Pipeline option " + property + " must be one of "
                        + java.util.Arrays.toString(type.getEnumConstants()) + ", but: " + s, e);
            }
        }
        if(value instanceof Number n) {
            if(type == int.class || type == Integer.class) {
                return n.intValue();
            } else if(type == long.class || type == Long.class) {
                return n.longValue();
            } else if(type == double.class || type == Double.class) {
                return n.doubleValue();
            } else if(type == float.class || type == Float.class) {
                return n.floatValue();
            }
        }
        if(value instanceof Map<?, ?> map && !type.isInstance(map)) {
            // typed override maps (SdkHarnessLogLevelOverrides, WorkerLogLevelOverrides) expose from(Map<String, String>)
            try {
                return type.getMethod("from", Map.class).invoke(null, map);
            } catch (final NoSuchMethodException | IllegalAccessException e) {
                throw new IllegalStateException("Pipeline option " + property + " does not accept a map", e);
            } catch (final InvocationTargetException e) {
                if(e.getCause() instanceof IllegalArgumentException iae) {
                    throw new IllegalArgumentException("Illegal value for pipeline option " + property + ": " + iae.getMessage(), iae);
                }
                throw new IllegalStateException("Failed to convert pipeline option " + property, e);
            }
        }
        return value;
    }

}
