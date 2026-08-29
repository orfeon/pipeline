package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Accumulates structured messages produced while compiling a feature spec.
 * Errors make the compile fail; warnings and hints are reported by {@code validate --expand}
 * and carried into the plan so an agent self-correction loop can act on them.
 */
public class Diagnostics implements Serializable {

    public enum Level { error, warning, hint, info }

    public record Message(Level level, String code, String location, String message) implements Serializable {
        @Override
        public String toString() {
            return level + "[" + code + "] " + (location == null ? "" : location + ": ") + message;
        }
    }

    private final List<Message> messages = new ArrayList<>();

    public void error(final String code, final String location, final String message) {
        messages.add(new Message(Level.error, code, location, message));
    }

    public void warning(final String code, final String location, final String message) {
        messages.add(new Message(Level.warning, code, location, message));
    }

    public void hint(final String code, final String location, final String message) {
        messages.add(new Message(Level.hint, code, location, message));
    }

    public void info(final String code, final String location, final String message) {
        messages.add(new Message(Level.info, code, location, message));
    }

    public void addAll(final Diagnostics other) {
        messages.addAll(other.messages);
    }

    public boolean hasErrors() {
        return messages.stream().anyMatch(m -> m.level == Level.error);
    }

    public List<Message> getMessages() {
        return Collections.unmodifiableList(messages);
    }

    public List<Message> get(final Level level) {
        return messages.stream().filter(m -> m.level == level).toList();
    }

    public List<String> getErrorMessages() {
        return get(Level.error).stream().map(Message::toString).collect(Collectors.toList());
    }

}
