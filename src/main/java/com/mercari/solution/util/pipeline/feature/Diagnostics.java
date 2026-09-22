package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    /** The messages recorded, for the duplicate test: a scan of the list is quadratic over a plan with many columns. */
    private final Set<Message> recorded = new HashSet<>();

    /**
     * Adds a message unless an identical one (level, code, location and text) was added before: an expansion that runs
     * once per keySet / window / column raises the same advice for each of them, and one line of it says as much as
     * five (a message that differs in any word - a column name, a count - is kept). Errors are never merged: each one
     * is a thing to fix, and an error that does not name its item (three malformed sources, one text) would otherwise
     * surface one at a time, a fix-and-rerun cycle per item.
     */
    private void add(final Message message) {
        if (message.level == Level.error) {
            messages.add(message);
            return;
        }
        if (recorded.add(message)) messages.add(message);
    }

    public void error(final String code, final String location, final String message) {
        add(new Message(Level.error, code, location, message));
    }

    public void warning(final String code, final String location, final String message) {
        add(new Message(Level.warning, code, location, message));
    }

    public void hint(final String code, final String location, final String message) {
        add(new Message(Level.hint, code, location, message));
    }

    public void info(final String code, final String location, final String message) {
        add(new Message(Level.info, code, location, message));
    }

    public void addAll(final Diagnostics other) {
        if (other == this) return;
        for (final Message m : other.messages) add(m);
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
