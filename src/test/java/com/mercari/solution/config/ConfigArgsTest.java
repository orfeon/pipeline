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
    public void testValuesAreEscapedForJsonAndRegex() throws Exception {
        // '$' and backslashes would be regex group references / escapes in a raw replaceAll; quotes would break the JSON text
        final Config config = Config.load(CONFIG, null, Config.Format.unknown,
                Map.of("dateFrom", "cost $5 \\ C:\\Users\\me \"quoted\""));
        Assertions.assertEquals("cost $5 \\ C:\\Users\\me \"quoted\"",
                config.getSources().get(0).getParameters().getAsJsonArray("elements").get(0).getAsJsonObject().get("id").getAsString());
        Assertions.assertEquals("run-cost $5 \\ C:\\Users\\me \"quoted\"", config.getSources().get(0).getArgs().get("label"));
    }

    @Test
    public void testSystemArgsAndTopLevelArgsAreMerged() throws Exception {
        final String both = CONFIG.replace("args:\n  dateFrom: \"2026-01-01\"\n", "system:\n  args:\n    dateFrom: \"2025-12-31\"\n    region: asia\nargs:\n  dateFrom: \"2026-01-01\"\n");
        final Config config = Config.load(both, null, Config.Format.unknown, (String) null);
        final Map<String, String> args = config.getSources().get(0).getArgs();
        Assertions.assertEquals("2026-01-01", args.get("dateFrom")); // top-level overrides system.args
        Assertions.assertEquals("asia", args.get("region"));         // system.args entries are kept
        Assertions.assertTrue(config.getContent().contains("\"id\":\"2026-01-01\""));
    }

    @Test
    public void testUnresolvedArgsCoverEverySpelling() {
        Assertions.assertEquals(java.util.List.of("dateFrom", "my-arg", "x?string"),
                Config.unresolvedArgs("a ${args.dateFrom} b ${args.my-arg} c ${args.x?string} d ${dateFrom} ${args.dateFrom}"));
        Assertions.assertTrue(Config.unresolvedArgs("nothing here ${other.x}").isEmpty());
    }

    @Test
    public void testLoadTimeArgsOverrideConfigDefaults() throws Exception {
        final Config config = Config.load(CONFIG, null, Config.Format.unknown, Map.of("dateFrom", "2027-05-05"));
        Assertions.assertTrue(config.getContent().contains("\"id\":\"2027-05-05\""), config.getContent());
        Assertions.assertEquals("2027-05-05", config.getSources().get(0).getArgs().get("dateFrom"));
        Assertions.assertEquals("run-2027-05-05", config.getSources().get(0).getArgs().get("label"));
    }

    @Test
    public void testContentStatesTheEffectiveArgs() throws Exception {
        // the config text kept with a job must show the values it ran with, not the defaults they overrode
        final Config config = Config.load(CONFIG, null, Config.Format.unknown, Map.of("dateFrom", "2027-05-05", "extra", "x"));
        final com.google.gson.JsonObject content = com.google.gson.JsonParser.parseString(config.getContent()).getAsJsonObject();
        final com.google.gson.JsonObject args = content.getAsJsonObject("args");
        Assertions.assertEquals("2027-05-05", args.get("dateFrom").getAsString(), config.getContent());
        Assertions.assertEquals("run-2027-05-05", args.get("label").getAsString(), config.getContent());
        // an argument passed at load time only is recorded under system.args
        Assertions.assertEquals("x", content.getAsJsonObject("system").getAsJsonObject("args").get("extra").getAsString(), config.getContent());

        // the content is what a job is launched with: loading it again, with or without the args, gives the same values
        for (final Map<String, String> again : java.util.List.of(Map.<String, String>of(), Map.of("dateFrom", "2027-05-05", "extra", "x"))) {
            final Config reloaded = Config.load(config.getContent(), null, Config.Format.unknown, again);
            Assertions.assertEquals(config.getContent(), reloaded.getContent());
            Assertions.assertEquals("run-2027-05-05", reloaded.getSources().get(0).getArgs().get("label"));
            Assertions.assertEquals("x", reloaded.getSources().get(0).getArgs().get("extra"));
        }

        // a declaration in both blocks is updated in both; a config without args gets no args block
        final String both = CONFIG.replace("args:\n  dateFrom: \"2026-01-01\"\n", "system:\n  args:\n    dateFrom: \"2025-12-31\"\nargs:\n  dateFrom: \"2026-01-01\"\n");
        final com.google.gson.JsonObject bothContent = com.google.gson.JsonParser.parseString(
                Config.load(both, null, Config.Format.unknown, Map.of("dateFrom", "2027-05-05")).getContent()).getAsJsonObject();
        Assertions.assertEquals("2027-05-05", bothContent.getAsJsonObject("args").get("dateFrom").getAsString());
        Assertions.assertEquals("2027-05-05", bothContent.getAsJsonObject("system").getAsJsonObject("args").get("dateFrom").getAsString());
        // a value that still reads as a template is not written back: loading the content must not evaluate it twice
        final com.google.gson.JsonObject kept = new com.google.gson.JsonObject();
        kept.add("args", new com.google.gson.JsonObject());
        kept.getAsJsonObject("args").addProperty("raw", "as written");
        Config.writeResolvedArgs(kept, Map.of("raw", "${later}", "passed", "${later}"));
        Assertions.assertEquals("as written", kept.getAsJsonObject("args").get("raw").getAsString());
        Assertions.assertFalse(kept.has("system"), kept::toString);
        final String none = CONFIG.substring(CONFIG.indexOf("sources:")).replace("${args.dateFrom}", "a");
        Assertions.assertFalse(Config.load(none, null, Config.Format.unknown, (String) null).getContent().contains("\"args\""));
    }

}
