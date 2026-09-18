package com.mercari.solution.util.domain.file;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class YamlUtilTest {

    @Test
    public void testYaml12CoreSchema() {
        final String yaml = """
                date: 2024-01-01
                timestamp: 2024-01-01T00:00:00Z
                yes: no
                on: off
                time: 1:30
                leadingZero: 0123
                octal: 0o17
                hex: 0x1F
                version: 1.10
                big: 12345678901234567890
                bool: True
                tilde: ~
                empty:
                quoted: "true"
                1: numberKey
                """;
        final JsonObject json = YamlUtil.toJson(yaml).getAsJsonObject();

        // YAML 1.1 implicit types that YAML 1.2 keeps as strings
        Assertions.assertEquals("2024-01-01", json.get("date").getAsString());
        Assertions.assertEquals("2024-01-01T00:00:00Z", json.get("timestamp").getAsString());
        Assertions.assertEquals("no", json.get("yes").getAsString());
        Assertions.assertEquals("off", json.get("on").getAsString());
        Assertions.assertEquals("1:30", json.get("time").getAsString());

        // core schema numbers
        Assertions.assertEquals(123, json.get("leadingZero").getAsInt());
        Assertions.assertEquals(15, json.get("octal").getAsInt());
        Assertions.assertEquals(31, json.get("hex").getAsInt());
        Assertions.assertEquals(1.1, json.get("version").getAsDouble());
        Assertions.assertEquals("12345678901234567890", json.get("big").getAsBigInteger().toString());

        Assertions.assertTrue(json.get("bool").getAsBoolean());
        Assertions.assertTrue(json.get("tilde").isJsonNull());
        Assertions.assertTrue(json.get("empty").isJsonNull());
        Assertions.assertTrue(json.get("quoted").getAsJsonPrimitive().isString());
        Assertions.assertEquals("numberKey", json.get("1").getAsString());
    }

    @Test
    public void testNestedAndJsonCompatible() {
        final String yaml = """
                sources:
                  - name: input
                    module: create
                    parameters: {"type": "string", "elements": ["a", "b"]}
                """;
        final JsonObject json = YamlUtil.toJson(yaml).getAsJsonObject();
        final JsonObject source = json.getAsJsonArray("sources").get(0).getAsJsonObject();
        Assertions.assertEquals("create", source.get("module").getAsString());
        Assertions.assertEquals(2, source.getAsJsonObject("parameters").getAsJsonArray("elements").size());
    }

    @Test
    public void testDuplicateKeysRejected() {
        Assertions.assertThrows(RuntimeException.class, () -> YamlUtil.load("a: 1\na: 2\n"));
    }

    @Test
    public void testConfigConvert() {
        final String yaml = """
                system:
                  args:
                    date: 2024-01-01
                sources:
                  - name: input
                    module: create
                """;
        final JsonObject yamlJson = Config.convertConfigJson(yaml, Config.Format.yaml);
        Assertions.assertEquals("2024-01-01", yamlJson.getAsJsonObject("system")
                .getAsJsonObject("args").get("date").getAsString());
        // unknown format falls back to yaml when the text is not json
        Assertions.assertEquals(yamlJson, Config.convertConfigJson(yaml, Config.Format.unknown));

        Assertions.assertThrows(IllegalModuleException.class,
                () -> Config.convertConfigJson("- a\n- b\n", Config.Format.yaml));
    }

}
