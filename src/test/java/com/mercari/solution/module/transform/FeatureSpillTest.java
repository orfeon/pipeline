package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.pipeline.feature.FeatureStages;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Map;

/**
 * The keyed stages sort each key's rows with Beam's external sorter and stream them: a hot key (here one
 * seller with every row, i.e. the shape of a shrinkage lattice's global level) must not need the whole key in
 * memory. The sorter buffer is forced to 1 MB so the rows actually spill to local disk, and the values are
 * checked against the closed form of the replay (running count / mean over strictly-past rows).
 */
@Execution(ExecutionMode.SAME_THREAD) // sets a JVM-wide system property while assembling
public class FeatureSpillTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final int ROWS = 20_000;

    /** JSON rather than YAML: SnakeYAML caps documents at ~3 MB and 20k rows exceed that. */
    private static String config() {
        final StringBuilder elements = new StringBuilder();
        // rows are created out of order (reverse) so the sort is exercised, all on one key
        for (int i = ROWS - 1; i >= 0; i--) {
            final long millis = 1_700_000_000_000L + i * 60_000L; // one row per minute
            if (elements.length() > 0) elements.append(",\n");
            elements.append("{\"session_id\": \"r").append(i)
                    .append("\", \"seller_id\": \"hot\", \"start_price\": ").append(i)
                    .append(".0, \"note\": \"").append("x".repeat(64)) // widen the row so 20k rows exceed 1 MB
                    .append("\", \"session_time\": \"").append(java.time.Instant.ofEpochMilli(millis)).append("\"}");
        }
        return """
                {
                  "sources": [{
                    "name": "create",
                    "module": "create",
                    "parameters": {
                      "type": "element",
                      "elements": [
                %s
                      ],
                      "schema": {"fields": [
                        {"name": "session_id", "type": "string"},
                        {"name": "seller_id", "type": "string"},
                        {"name": "start_price", "type": "float64"},
                        {"name": "note", "type": "string"},
                        {"name": "session_time", "type": "timestamp"}
                      ]}
                    }
                  }],
                  "transforms": [{
                    "name": "features",
                    "module": "feature",
                    "inputs": ["create"],
                    "parameters": {
                      "sources": [{
                        "name": "listings",
                        "eventTime": "session_time",
                        "availability": "atEventTime",
                        "fields": [
                          {"name": "session_id", "type": "string"},
                          {"name": "seller_id", "type": "string"},
                          {"name": "start_price", "type": "float64"},
                          {"name": "note", "type": "string"}
                        ]
                      }],
                      "lineage": [{"fields": ["session_id", "seller_id", "start_price", "note"], "from": "listings"}],
                      "time": {"field": "session_time"},
                      "predictAt": "event_time - PT1M",
                      "entities": [{"name": "seller", "keys": ["seller_id"]}],
                      "features": [
                        {"name": "hist", "scope": "sequence", "entity": "seller",
                         "windows": [{"maxAge": "P1D"}],
                         "ops": [{"type": "aggregate", "field": "start_price", "funcs": ["count", "mean"]}]},
                        {"name": "enc", "scope": "population", "type": "encoding",
                         "keySets": [{"keys": ["seller_id"]}],
                         "targets": [{"stats": ["count"]}]}
                      ],
                      "output": {"prefix": "f_"}
                    }
                  }]
                }
                """.formatted(elements);
    }

    @Test
    public void testHotKeySpillsToDiskAndReplaysInOrder() throws Exception {
        final String previous = System.getProperty(FeatureStages.SORTER_MEMORY_PROPERTY);
        System.setProperty(FeatureStages.SORTER_MEMORY_PROPERTY, "1");
        final Map<String, MCollection> outputs;
        try {
            outputs = MPipeline.apply(pipeline, Config.load(config()));
        } finally {
            if (previous == null) System.clearProperty(FeatureStages.SORTER_MEMORY_PROPERTY);
            else System.setProperty(FeatureStages.SORTER_MEMORY_PROPERTY, previous);
        }
        PAssert.that(outputs.get("features").getCollection()).satisfies(rows -> {
            int n = 0;
            for (final MElement row : rows) {
                n++;
                final int i = Integer.parseInt(row.getAsString("session_id").substring(1));
                // window [event_time - 1 day, event_time - 1 min]: rows i-1440 .. i-1 → count = min(i, 1440)
                final long count = ((Number) row.getPrimitiveValue("f_hist_1d_start_price_count")).longValue();
                Assertions.assertEquals(Math.min(i, 1440), count, "row " + i);
                if (i > 0) {
                    final int lo = Math.max(0, i - 1440);
                    final double mean = (lo + (i - 1)) / 2.0; // start_price == index
                    Assertions.assertEquals(mean, row.getAsDouble("f_hist_1d_start_price_mean"), 1e-9, "row " + i);
                }
                // unbounded population count sees every strictly-past row of the key
                Assertions.assertEquals(i, ((Number) row.getPrimitiveValue("f_enc__seller_id__count")).longValue(), "row " + i);
            }
            Assertions.assertEquals(ROWS, n);
            return null;
        });
        pipeline.run();
    }

}
