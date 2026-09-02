package com.mercari.solution.config.options;

import com.mercari.solution.config.Options;
import com.mercari.solution.util.domain.file.JsonUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class PrismOptionsTest {

    private static final String BUNDLED = "/opt/prism/apache_beam-v2.76.0-prism-linux-amd64";

    /** The bundled binary is a default: a config value (even an empty one) or a command-line value wins. */
    @Test
    public void testBundledBinaryIsOnlyADefault() {
        Assertions.assertEquals(BUNDLED, PrismOptions.defaultPrismLocation(null, null, BUNDLED));
        Assertions.assertEquals(BUNDLED, PrismOptions.defaultPrismLocation(null, "", " " + BUNDLED + " "), "trimmed");
        // --prismLocation on the command line (already in the options) is kept
        Assertions.assertNull(PrismOptions.defaultPrismLocation(null, "/mnt/prism/bin", BUNDLED));
        // options.prism.prismLocation in the config is kept; "" re-enables the runner's own download
        Assertions.assertNull(PrismOptions.defaultPrismLocation("/mnt/prism/bin", null, BUNDLED));
        Assertions.assertNull(PrismOptions.defaultPrismLocation("", null, BUNDLED));
        // no bundled binary (outside the prism image): nothing to apply
        Assertions.assertNull(PrismOptions.defaultPrismLocation(null, null, null));
        Assertions.assertNull(PrismOptions.defaultPrismLocation(null, null, " "));
    }

    /** Without a bundled binary and without a prism block nothing touches the options (no prism runner classes needed). */
    @Test
    public void testNoPrismBlockAndNoBundledBinaryIsANoOp() {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        PrismOptions.setOptions(pipelineOptions, null, null);
        PrismOptions.setOptions(pipelineOptions, null, "");
    }

    // The PrismPipelineOptions class is referenced by name only (as in PrismOptions itself), so this test
    // compiles under every runner profile and is skipped where the prism runner is absent.
    @SuppressWarnings("unchecked")
    @Test
    public void testSetOptionsAppliesTheDefaultAndTheOverrides() throws Exception {
        final Class<?> clazz;
        try {
            clazz = Class.forName("org.apache.beam.runners.prism.PrismPipelineOptions");
        } catch (final ClassNotFoundException e) {
            Assumptions.abort("prism runner not on the classpath (build with -Pprism to run this test)");
            return;
        }
        // bundled default, no config block
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        PrismOptions.setOptions(pipelineOptions, null, BUNDLED);
        Assertions.assertEquals(BUNDLED, prismLocation(pipelineOptions, clazz));

        // command line wins over the bundled default
        pipelineOptions = PipelineOptionsFactory.fromArgs("--prismLocation=/mnt/prism/bin").create();
        PrismOptions.setOptions(pipelineOptions, null, BUNDLED);
        Assertions.assertEquals("/mnt/prism/bin", prismLocation(pipelineOptions, clazz));

        // config wins over both; an empty config value re-enables the download
        pipelineOptions = PipelineOptionsFactory.fromArgs("--prismLocation=/mnt/prism/bin").create();
        Options options = JsonUtil.fromJson("{\"prism\":{\"prismLocation\":\"/cfg/prism\",\"idleShutdownTimeout\":\"15m\"}}", Options.class);
        PrismOptions.setOptions(pipelineOptions, options.getPrism(), BUNDLED);
        Assertions.assertEquals("/cfg/prism", prismLocation(pipelineOptions, clazz));
        Assertions.assertEquals("15m", clazz.getMethod("getIdleShutdownTimeout").invoke(pipelineOptions.as((Class<? extends PipelineOptions>) clazz)));

        pipelineOptions = PipelineOptionsFactory.create();
        options = JsonUtil.fromJson("{\"prism\":{\"prismLocation\":\"\",\"prismVersionOverride\":\"2.76.0\"}}", Options.class);
        PrismOptions.setOptions(pipelineOptions, options.getPrism(), BUNDLED);
        Assertions.assertEquals("", prismLocation(pipelineOptions, clazz));
    }

    @SuppressWarnings("unchecked")
    private static String prismLocation(final PipelineOptions pipelineOptions, final Class<?> clazz) throws Exception {
        return (String) clazz.getMethod("getPrismLocation").invoke(pipelineOptions.as((Class<? extends PipelineOptions>) clazz));
    }

}
