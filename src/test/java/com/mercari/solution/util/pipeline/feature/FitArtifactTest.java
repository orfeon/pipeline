package com.mercari.solution.util.pipeline.feature;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FitArtifactTest {

    /** The artifact schema before the score-type offset estimator: no {@code sumInfo} statistic. */
    private static final Schema BEFORE_INFO = new Schema.Parser().parse("""
            {"type": "record", "name": "FeatureFitStats", "namespace": "com.mercari.solution.feature",
             "fields": [
               {"name": "level", "type": "string"},
               {"name": "key", "type": "string"},
               {"name": "n", "type": "double"},
               {"name": "sum", "type": "double"},
               {"name": "sumSq", "type": "double"},
               {"name": "sumOff", "type": "double", "default": 0.0}
             ]}
            """);

    /**
     * The statistics round-trip through the artifact, Σ b(1 − b) included; an artifact written before that statistic
     * existed reads 0 for it — accepted by a level that does not need it (identity / log: the information is Σb), refused
     * with a refit advice by a logit offset term, which would otherwise read no information for every key.
     */
    @Test
    public void testArtifactCarriesInformationAndRefusesAnOlderOneWhenRequired() throws Exception {
        final String dir = "target/feature-artifacts/" + java.util.UUID.randomUUID();
        final Map<String, VarianceComponents.KeyStats> stats = new HashMap<>();
        final VarianceComponents.KeyStats s = new VarianceComponents.KeyStats();
        s.n = 4;
        s.sum = 1;
        s.sumSq = 3;
        s.sumOff = 2;
        s.sumInfo = 0.8;
        stats.put(FitArtifact.entryKey("enc__seller__n", "s1"), s);
        FitArtifact.write(dir, "hash", "enc", stats, List.of("enc__seller__n"), null, Map.of("enc__seller__n", "logit"));
        final VarianceComponents.KeyStats read = FitArtifact.read(dir, "hash", "enc", true).get(FitArtifact.entryKey("enc__seller__n", "s1"));
        Assertions.assertEquals(0.8, read.sumInfo, 0d);
        Assertions.assertEquals(2, read.sumOff, 0d);

        // an artifact of the older schema at the path of another block
        final File older = new File(FitArtifact.statsPath(dir, "hash", "old"));
        try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(BEFORE_INFO))) {
            writer.create(BEFORE_INFO, older);
            final GenericRecord record = new GenericData.Record(BEFORE_INFO);
            record.put("level", "old__seller__n");
            record.put("key", "s1");
            record.put("n", 4d);
            record.put("sum", 1d);
            record.put("sumSq", 3d);
            record.put("sumOff", 2d);
            writer.append(record);
        }
        Assertions.assertEquals(0d, FitArtifact.read(dir, "hash", "old", false).get(FitArtifact.entryKey("old__seller__n", "s1")).sumInfo, 0d);
        final IllegalStateException refused = Assertions.assertThrows(IllegalStateException.class, () -> FitArtifact.read(dir, "hash", "old", true));
        Assertions.assertTrue(refused.getMessage().contains("refit"), refused.getMessage());
    }
}
