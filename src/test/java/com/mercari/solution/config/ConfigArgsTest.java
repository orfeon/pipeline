package com.mercari.solution.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Template arguments: the config's own {@code args} defaults feed both the load-time {@code ${args.<name>}}
 * substitution and the modules' runtime template arguments ({@code ${<name>}} inside module templates),
 * with load-time args overriding them.
 */
public class ConfigArgsTest {

    private static final String CONFIG = """
            args:
              dateFrom: "2026-01-01"
              label: "run-${args.dateFrom}"
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  schema:
                    fields:
                      - {name: id, type: string}
                  elements:
                    - {id: "${args.dateFrom}"}
            sinks:
              - name: out
                module: debug
                inputs: [input]
            """;

    @Test
    public void testConfigArgsAreModuleTemplateArgs() throws Exception {
        final Config config = Config.load(CONFIG, null, Config.Format.unknown, "{\"dateTo\": \"2026-02-01\"}");
        Assertions.assertTrue(config.getContent().contains("\"id\":\"2026-01-01\""), config.getContent());
        final Map<String, String> args = config.getSources().get(0).getArgs();
        Assertions.assertEquals("2026-01-01", args.get("dateFrom"), args::toString);   // config default
        Assertions.assertEquals("run-2026-01-01", args.get("label"), args::toString); // evaluated against earlier args
        Assertions.assertEquals("2026-02-01", args.get("dateTo"), args::toString);     // load-time arg
        Assertions.assertEquals(args, config.getSinks().get(0).getArgs());
    }

    @Test
    public void testLoadTimeArgsOverrideConfigDefaults() throws Exception {
        final Config config = Config.load(CONFIG, null, Config.Format.unknown, Map.of("dateFrom", "2027-05-05"));
        Assertions.assertTrue(config.getContent().contains("\"id\":\"2027-05-05\""), config.getContent());
        Assertions.assertEquals("2027-05-05", config.getSources().get(0).getArgs().get("dateFrom"));
        Assertions.assertEquals("run-2027-05-05", config.getSources().get(0).getArgs().get("label"));
    }

}
