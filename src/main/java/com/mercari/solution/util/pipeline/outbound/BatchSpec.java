package com.mercari.solution.util.pipeline.outbound;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Micro-batching configuration shared by the outbound sinks (http / grpc / tasks): group input
 * elements into one request with {@link GroupIntoBatches}, keyed by a rendered template or by
 * random shards.
 */
public class BatchSpec implements Serializable {

    public Integer maxSize;
    public String maxBytes;
    public String maxBufferingDuration;
    public String key;
    public Integer shards;

    public List<String> validate(final String prefix) {
        final List<String> errorMessages = new ArrayList<>();
        if(maxSize != null && maxSize < 1) {
            errorMessages.add(prefix + ".maxSize must be >= 1 but: " + maxSize);
        }
        if(maxBytes != null) {
            try {
                Durations.parseBytes(maxBytes);
            } catch (final IllegalArgumentException e) {
                errorMessages.add(prefix + ".maxBytes is illegal: " + e.getMessage());
            }
        }
        if(maxSize == null && maxBytes == null) {
            errorMessages.add(prefix + " requires maxSize and/or maxBytes");
        }
        if(maxBufferingDuration != null) {
            try {
                Durations.parse(maxBufferingDuration);
            } catch (final IllegalArgumentException e) {
                errorMessages.add(prefix + ".maxBufferingDuration is illegal: " + e.getMessage());
            }
        }
        if(shards != null && shards < 1) {
            errorMessages.add(prefix + ".shards must be >= 1 but: " + shards);
        }
        if(key != null && !TemplateUtil.isTemplateText(key)) {
            errorMessages.add(prefix + ".key must be a template on element fields but: " + key);
        }
        return errorMessages;
    }

    /**
     * Elements of one batch share the rendered per-request templates, so those templates may only
     * reference fields that also appear in {@code key} (elements in a batch are equal on them).
     */
    public List<String> validateKeyConstraint(final String prefix, final Schema inputSchema, final Map<String, String> perRequestTemplates) {
        final List<String> errorMessages = new ArrayList<>();
        final Set<String> keyArgs = new HashSet<>();
        if(key != null) {
            keyArgs.addAll(TemplateUtil.extractTemplateArgs(key, inputSchema));
        }
        for(final Map.Entry<String, String> entry : perRequestTemplates.entrySet()) {
            if(entry.getValue() == null) {
                continue;
            }
            for(final String arg : TemplateUtil.extractTemplateArgs(entry.getValue(), inputSchema)) {
                if(!keyArgs.contains(arg)) {
                    errorMessages.add(entry.getKey() + " references field '" + arg
                            + "' which is not part of " + prefix + ".key (in batch mode per-request templates may only use batch.key fields)");
                }
            }
        }
        return errorMessages;
    }

    public void setDefaults() {
        if(shards == null) {
            shards = 8;
        }
    }

    public Long maxBytesValue() {
        return maxBytes == null ? null : Durations.parseBytes(maxBytes);
    }

    public GroupIntoBatches<String, MElement> groupIntoBatches() {
        final Long bytes = maxBytesValue();
        GroupIntoBatches<String, MElement> group = maxSize != null
                ? GroupIntoBatches.ofSize(maxSize.longValue())
                : GroupIntoBatches.ofByteSize(bytes);
        if(maxSize != null && bytes != null) {
            group = group.withByteSize(bytes);
        }
        if(maxBufferingDuration != null) {
            group = group.withMaxBufferingDuration(org.joda.time.Duration.millis(Durations.parse(maxBufferingDuration).toMillis()));
        }
        return group;
    }

    /** Renders the batch key of an element (null when the spec has no key). Implementations compile templates in {@link #setup()}. */
    public interface KeyRenderer extends Serializable {
        void setup();
        String render(MElement element);
    }

    /** Assigns the batch grouping key: the rendered key, or a random shard when the key is omitted. */
    public static class KeyDoFn extends DoFn<MElement, KV<String, MElement>> {

        private final KeyRenderer renderer;
        private final int shards;

        public KeyDoFn(final KeyRenderer renderer, final int shards) {
            this.renderer = renderer;
            this.shards = shards;
        }

        @Setup
        public void setup() {
            renderer.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            final String key = renderer.render(input);
            c.output(KV.of(key != null ? key : "shard-" + ThreadLocalRandom.current().nextInt(shards), input));
        }
    }
}
